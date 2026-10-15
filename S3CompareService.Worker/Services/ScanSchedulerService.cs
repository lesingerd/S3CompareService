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

        // Wait a few seconds for HTTP server to settle
        await Task.Delay(2000, stoppingToken).ConfigureAwait(false);

        // Run crash recovery first
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var recovery = scope.ServiceProvider.GetRequiredService<CrashRecoveryManager>();
            await recovery.RunStartupRecoveryAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during crash recovery startup sequence.");
        }

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

    private async Task EvaluateSchedulesAsync(CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var jobRepo = scope.ServiceProvider.GetRequiredService<IJobRepository>();
        var scanRunRepo = scope.ServiceProvider.GetRequiredService<IScanRunRepository>();
        var alertService = scope.ServiceProvider.GetRequiredService<S3CompareService.Core.Notifications.IAlertNotificationService>();
        var coordinator = scope.ServiceProvider.GetRequiredService<ScanExecutionCoordinator>();

        var jobs = await jobRepo.GetAllAsync(stoppingToken).ConfigureAwait(false);
        var now = DateTime.UtcNow;

        foreach (var job in jobs)
        {
            if (!job.IsEnabled) continue;

            // Check Daily Status Report / All-Clear condition
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

                        await alertService.DispatchDailyDigestAsync(job, latestRun, runs24h, stoppingToken).ConfigureAwait(false);
                        job.AlertConfig.LastDailyDigestSentUtc = now;
                        await jobRepo.SaveAsync(job, stoppingToken).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error evaluating daily digest for Job '{JobName}'", job.Name);
                }
            }

            if (coordinator.IsJobRunning(job.Id)) continue;

            bool isDue = false;

            if (job.ScheduleType == JobScheduleType.IntervalMinutes)
            {
                if (job.IntervalMinutes <= 0) continue;

                if (job.LastRunTimeUtc == null)
                {
                    // Never run before: trigger initial run
                    isDue = true;
                }
                else
                {
                    var nextDue = job.LastRunTimeUtc.Value.AddMinutes(job.IntervalMinutes);
                    if (now >= nextDue)
                    {
                        isDue = true;
                    }
                    job.NextRunTimeUtc = nextDue;
                }
            }
            else if (job.ScheduleType == JobScheduleType.CronExpression && !string.IsNullOrWhiteSpace(job.CronExpression))
            {
                try
                {
                    var expression = CronExpression.Parse(job.CronExpression);
                    var nextUtc = expression.GetNextOccurrence(job.LastRunTimeUtc ?? now.AddMinutes(-1), TimeZoneInfo.Utc);
                    if (nextUtc.HasValue)
                    {
                        job.NextRunTimeUtc = nextUtc.Value;
                        if (now >= nextUtc.Value)
                        {
                            isDue = true;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Invalid cron expression '{Cron}' for Job '{JobName}': {Error}", job.CronExpression, job.Name, ex.Message);
                }
            }

            if (isDue)
            {
                _logger.LogInformation("Job '{JobName}' is due for execution. Triggering scan...", job.Name);
                await coordinator.TriggerScanAsync(job.Id).ConfigureAwait(false);
            }
        }
    }
}
