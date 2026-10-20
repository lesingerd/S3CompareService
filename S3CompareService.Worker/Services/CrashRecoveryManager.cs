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

    /// <summary>
    /// Finds runs left Running/Paused/Interrupted by a crash or graceful shutdown and resumes each one IN PLACE
    /// (same run id) when it has a checkpoint and its job is enabled. Runs that cannot be resumed are marked Interrupted.
    /// Runs this process already executes are skipped.
    /// </summary>
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

        _logger.LogWarning("Found {Count} unfinished scan run(s). Initiating recovery sequence...", unfinishedRuns.Count);

        foreach (var run in unfinishedRuns)
        {
            if (_coordinator.IsRunActive(run.Id)) continue; // live in this process

            var checkpoint = await _scanRunRepo.GetCheckpointAsync(run.Id, cancellationToken);
            string lastKey = checkpoint?.LastEvaluatedKey ?? run.LastEvaluatedKey;

            string message = $"Prior scan run '{run.Id}' for Job '{run.JobName}' was interrupted by unexpected service termination, shutdown or system restart.";
            string details = $"Status before recovery: {run.Status}. Evaluated: {run.ObjectsCompared:N0}. Last evaluated key: '{lastKey}'. Checkpoint exists: {checkpoint != null}.";
            _logger.LogWarning("{Message} {Details}", message, details);

            await _auditLogRepo.LogAsync("RECOVERY", "CrashRecovery", message, details: details, scanRunId: run.Id, jobId: run.JobId, cancellationToken: cancellationToken);

            if (checkpoint != null && !string.IsNullOrWhiteSpace(checkpoint.LastEvaluatedKey))
            {
                var result = await _coordinator.ResumeUnfinishedRunAsync(run.Id);
                if (result.Success)
                {
                    _logger.LogInformation("Auto-resumed run {RunId} for Job '{JobName}' after key '{LastKey}'.", run.Id, run.JobName, checkpoint.LastEvaluatedKey);
                    continue;
                }
                if (result.Failure == TriggerFailure.AlreadyRunning || result.Failure == TriggerFailure.ShuttingDown)
                {
                    continue; // leave state untouched; picked up on a later start
                }
                _logger.LogInformation("Run {RunId} not auto-resumed: {Reason}", run.Id, result.Message);
            }

            if (run.Status != RunStatus.Interrupted)
            {
                run.Status = RunStatus.Interrupted;
                run.EndTimeUtc = DateTime.UtcNow;
                run.ErrorMessage = "Scan interrupted by service restart / system reboot. Recovery checkpoint recorded.";
                await _scanRunRepo.CompleteAsync(run, cancellationToken);
            }
        }
    }
}
