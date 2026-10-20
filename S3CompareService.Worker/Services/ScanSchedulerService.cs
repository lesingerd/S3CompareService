using Cronos;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Worker.Services;

public class ScanSchedulerService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ScanSchedulerService> _logger;

    public ScanSchedulerService(IServiceProvider serviceProvider, ILogger<ScanSchedulerService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ScanSchedulerService background service started.");

        // Crash recovery runs in Program.cs before the server accepts requests; the scheduler only evaluates schedules.
        await Task.Delay(2000, stoppingToken).ConfigureAwait(false);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await EvaluateSchedulesAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in scheduler evaluation loop.");
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _logger.LogInformation("ScanSchedulerService background service shutting down.");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        var coordinator = _serviceProvider.GetRequiredService<ScanExecutionCoordinator>();

        // 1. Refuse new scans (scheduler, API, recovery) before touching the active ones.
        coordinator.BeginShutdown();

        // 2. Stop the scheduler loop so it cannot race the shutdown.
        try
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while stopping the scheduler loop.");
        }

        // 3. Interrupt active scans and give them time to write their checkpoints (host ShutdownTimeout is 60s).
        try
        {
            await coordinator.StopAllActiveScansAsync(cancellationToken, TimeSpan.FromSeconds(45)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while stopping active scans during service shutdown.");
        }
    }

    /// <summary>
    /// Pure due-time computation. <paramref name="lastStartUtc"/> is the start of the most recent run (interval anchor);
    /// when null the job's LastRunTimeUtc is used. All inputs are treated as UTC.
    /// </summary>
    public static (bool IsDue, DateTime? NextRunUtc) ComputeSchedule(ComparisonJob job, DateTime nowUtc, DateTime? lastStartUtc = null)
    {
        if (job.ScheduleType == JobScheduleType.IntervalMinutes)
        {
            if (job.IntervalMinutes <= 0) return (false, null);

            // Anchor on when the previous run started, not when it finished, so the cadence does not drift by run duration.
            var anchor = lastStartUtc ?? job.LastRunTimeUtc;
            if (anchor == null) return (true, null); // never run before: initial run

            var nextDue = AsUtc(anchor.Value).AddMinutes(job.IntervalMinutes);
            return (nowUtc >= nextDue, nextDue);
        }

        if (job.ScheduleType == JobScheduleType.CronExpression && !string.IsNullOrWhiteSpace(job.CronExpression))
        {
            var expression = CronExpression.Parse(job.CronExpression);
            var baseTime = AsUtc(job.LastRunTimeUtc ?? job.CreatedAtUtc);
            var nextUtc = expression.GetNextOccurrence(baseTime, TimeZoneInfo.Utc);
            if (nextUtc.HasValue)
                return (nowUtc >= nextUtc.Value, nextUtc.Value);
        }

        return (false, null);
    }

    private static DateTime AsUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Utc => value,
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => DateTime.SpecifyKind(value, DateTimeKind.Utc)
    };

    private async Task<(bool IsDue, DateTime? NextRunUtc)> ComputeForJobAsync(ComparisonJob job, DateTime now, IScanRunRepository scanRunRepo, CancellationToken ct)
    {
        DateTime? lastStart = null;
        if (job.ScheduleType == JobScheduleType.IntervalMinutes)
        {
            var latest = await scanRunRepo.GetRecentRunsAsync(1, job.Id, ct).ConfigureAwait(false);
            lastStart = latest.FirstOrDefault()?.StartTimeUtc;
        }
        return ComputeSchedule(job, now, lastStart);
    }

    private async Task EvaluateSchedulesAsync(CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var jobRepo = scope.ServiceProvider.GetRequiredService<IJobRepository>();
        var scanRunRepo = scope.ServiceProvider.GetRequiredService<IScanRunRepository>();
        var alertService = scope.ServiceProvider.GetRequiredService<S3CompareService.Core.Notifications.IAlertNotificationService>();
        var coordinator = scope.ServiceProvider.GetRequiredService<ScanExecutionCoordinator>();

        var jobIds = (await jobRepo.GetAllAsync(stoppingToken).ConfigureAwait(false)).Select(j => j.Id).ToList();

        foreach (var jobId in jobIds)
        {
            if (stoppingToken.IsCancellationRequested || coordinator.IsStopping) break;

            // Always evaluate against current database state, never the list snapshot.
            var job = await jobRepo.GetByIdAsync(jobId, stoppingToken).ConfigureAwait(false);
            if (job == null || !job.IsEnabled) continue;
            var now = DateTime.UtcNow;

            // Daily status report / all-clear
            if (job.AlertConfig.IsEnabled && job.AlertConfig.SendDailyDigest)
            {
                try
                {
                    var (shouldSendDigest, _) = await alertService.EvaluateDailyDigestConditionAsync(job, now).ConfigureAwait(false);
                    if (shouldSendDigest)
                    {
                        _logger.LogInformation("Job '{JobName}' is due for daily all-clear status report. Dispatching...", job.Name);
                        var recentRuns = await scanRunRepo.GetRecentRunsAsync(100, job.Id, stoppingToken).ConfigureAwait(false);
                        var runs24h = recentRuns.Where(r => r.StartTimeUtc >= now.AddHours(-24)).ToList();
                        var latestRun = recentRuns.FirstOrDefault();

                        bool delivered = await alertService.DispatchDailyDigestAsync(job, latestRun, runs24h, stoppingToken).ConfigureAwait(false);
                        if (delivered)
                        {
                            await jobRepo.UpdateAlertTimestampsAsync(job.Id, null, DateTime.UtcNow, stoppingToken).ConfigureAwait(false);
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error evaluating daily digest for Job '{JobName}'", job.Name);
                }
            }

            if (coordinator.IsJobRunning(job.Id)) continue;

            (bool isDue, DateTime? nextRun) schedule;
            try
            {
                schedule = await ComputeForJobAsync(job, now, scanRunRepo, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Invalid schedule for Job '{JobName}' (cron '{Cron}'): {Error}", job.Name, job.CronExpression, ex.Message);
                continue;
            }

            if (schedule.nextRun.HasValue && job.NextRunTimeUtc != schedule.nextRun)
            {
                await jobRepo.UpdateNextRunTimeAsync(job.Id, schedule.nextRun, stoppingToken).ConfigureAwait(false);
            }

            if (!schedule.isDue || coordinator.IsStopping) continue;

            // Re-read right before triggering: the job may have been edited, disabled, deleted or just run.
            var fresh = await jobRepo.GetByIdAsync(job.Id, stoppingToken).ConfigureAwait(false);
            if (fresh == null || !fresh.IsEnabled || coordinator.IsJobRunning(fresh.Id)) continue;
            var recheck = await ComputeForJobAsync(fresh, DateTime.UtcNow, scanRunRepo, stoppingToken).ConfigureAwait(false);
            if (!recheck.IsDue) continue;

            _logger.LogInformation("Job '{JobName}' is due for execution. Triggering scan...", fresh.Name);
            var result = await coordinator.TriggerScanAsync(fresh.Id).ConfigureAwait(false);
            if (!result.Success && result.Failure != TriggerFailure.AlreadyRunning)
            {
                _logger.LogWarning("Scheduled trigger for Job '{JobName}' was not started: {Message}", fresh.Name, result.Message);
            }
        }
    }
}
