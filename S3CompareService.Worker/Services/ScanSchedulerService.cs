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
        var coordinator = scope.ServiceProvider.GetRequiredService<ScanExecutionCoordinator>();

        var jobs = await jobRepo.GetAllAsync(stoppingToken).ConfigureAwait(false);
        var now = DateTime.UtcNow;

        foreach (var job in jobs)
        {
            if (!job.IsEnabled) continue;
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
