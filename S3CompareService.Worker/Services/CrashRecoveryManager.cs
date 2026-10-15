using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Worker.Services;

public class CrashRecoveryManager
{
    private readonly IScanRunRepository _scanRunRepo;
    private readonly IJobRepository _jobRepo;
    private readonly IAuditLogRepository _auditLogRepo;
    private readonly ScanExecutionCoordinator _coordinator;
    private readonly ILogger<CrashRecoveryManager> _logger;

    public CrashRecoveryManager(
        IScanRunRepository scanRunRepo,
        IJobRepository jobRepo,
        IAuditLogRepository auditLogRepo,
        ScanExecutionCoordinator coordinator,
        ILogger<CrashRecoveryManager> logger)
    {
        _scanRunRepo = scanRunRepo;
        _jobRepo = jobRepo;
        _auditLogRepo = auditLogRepo;
        _coordinator = coordinator;
        _logger = logger;
    }

    public async Task RunStartupRecoveryAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Scanning for interrupted scans from prior service execution or system crash...");
        var unfinishedRuns = await _scanRunRepo.GetUnfinishedRunsAsync(cancellationToken);

        if (unfinishedRuns.Count == 0)
        {
            _logger.LogInformation("Clean state: No interrupted runs detected.");
            await _auditLogRepo.LogAsync("INFO", "CrashRecovery", "Service started cleanly. No interrupted scan runs found.", cancellationToken: cancellationToken);
            return;
        }

        _logger.LogWarning("Found {Count} interrupted scan run(s). Initiating recovery sequence...", unfinishedRuns.Count);

        foreach (var run in unfinishedRuns)
        {
            var checkpoint = await _scanRunRepo.GetCheckpointAsync(run.Id, cancellationToken);
            string lastKey = checkpoint?.LastEvaluatedKey ?? run.LastEvaluatedKey;

            string message = $"Prior scan run '{run.Id}' for Job '{run.JobName}' was interrupted by unexpected service termination or system restart.";
            string details = $"Status before crash: {run.Status}. Evaluated: {run.ObjectsCompared:N0}. Last evaluated key: '{lastKey}'. Checkpoint exists: {checkpoint != null}.";

            _logger.LogWarning("{Message} {Details}", message, details);

            // Mark the old run as Interrupted
            run.Status = RunStatus.Interrupted;
            run.EndTimeUtc = DateTime.UtcNow;
            run.ErrorMessage = "Scan interrupted by service restart / system reboot. Recovery checkpoint recorded.";
            await _scanRunRepo.CompleteAsync(run, cancellationToken);

            await _auditLogRepo.LogAsync(
                "RECOVERY",
                "CrashRecovery",
                message,
                details: details,
                scanRunId: run.Id,
                jobId: run.JobId,
                cancellationToken: cancellationToken);

            // Auto-resume if checkpoint exists and job is enabled
            if (checkpoint != null && !string.IsNullOrWhiteSpace(checkpoint.LastEvaluatedKey))
            {
                var job = await _jobRepo.GetByIdAsync(run.JobId, cancellationToken);
                if (job != null && job.IsEnabled)
                {
                    _logger.LogInformation("Auto-resuming interrupted scan for Job '{JobName}' starting after key '{LastKey}'...", job.Name, checkpoint.LastEvaluatedKey);
                    await _coordinator.TriggerScanAsync(job.Id, checkpoint);
                }
            }
        }
    }
}
