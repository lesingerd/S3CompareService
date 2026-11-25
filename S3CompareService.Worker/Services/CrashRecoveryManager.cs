using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.IdleMon;

namespace S3CompareService.Worker.Services;

public class CrashRecoveryManager
{
    private readonly IScanRunRepository _scanRunRepo;
    private readonly IJobRepository _jobRepo;
    private readonly IAuditLogRepository _auditLogRepo;
    private readonly ScanExecutionCoordinator _coordinator;
    private readonly ILogger<CrashRecoveryManager> _logger;
    private readonly IIdleMonEvents _events;

    public CrashRecoveryManager(
        IScanRunRepository scanRunRepo,
        IJobRepository jobRepo,
        IAuditLogRepository auditLogRepo,
        ScanExecutionCoordinator coordinator,
        ILogger<CrashRecoveryManager> logger,
        IIdleMonEvents? events = null)
    {
        _scanRunRepo = scanRunRepo;
        _jobRepo = jobRepo;
        _auditLogRepo = auditLogRepo;
        _coordinator = coordinator;
        _logger = logger;
        _events = events ?? NoOpIdleMonEvents.Instance;
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
            bool canResume = checkpoint != null && checkpoint.CanResume;
            bool alreadyReported = run.Status == RunStatus.Interrupted;

            string details = $"Status before recovery: {run.Status}. Evaluated: {run.ObjectsCompared:N0}. Last evaluated key: '{lastKey}'. Checkpoint exists: {checkpoint != null}.";

            // No usable checkpoint (e.g. the service stopped while the run was still planning): it can never be resumed, so close
            // it now instead of leaving it Interrupted, where it would be picked up (and reported) again at every start.
            if (!canResume)
            {
                string closeMessage = $"Prior scan run '{run.Id}' for Job '{run.JobName}' was interrupted before it saved any progress, so it cannot be resumed. It is closed as Failed; the job's next scan starts from the beginning.";
                _logger.LogWarning("{Message} {Details}", closeMessage, details);
                await _auditLogRepo.LogAsync("RECOVERY", "CrashRecovery", closeMessage, details: details, scanRunId: run.Id, jobId: run.JobId, cancellationToken: cancellationToken);
                run.Status = RunStatus.Failed;
                run.EndTimeUtc ??= DateTime.UtcNow;
                run.ErrorMessage = "Scan interrupted by service restart / system reboot before any progress was saved (no checkpoint); it cannot be resumed.";
                await _scanRunRepo.CompleteAsync(run, cancellationToken);
                if (!alreadyReported) Notify(() => _events.RunRecovered(run, resumed: false));
                continue;
            }

            string message = $"Prior scan run '{run.Id}' for Job '{run.JobName}' was interrupted by unexpected service termination, shutdown or system restart.";
            _logger.LogWarning("{Message} {Details}", message, details);

            await _auditLogRepo.LogAsync("RECOVERY", "CrashRecovery", message, details: details, scanRunId: run.Id, jobId: run.JobId, cancellationToken: cancellationToken);

            // CanResume: a key (version 1) or any range progress in the partition plan (version 2; its frontier key may be empty).
            {
                var result = await _coordinator.ResumeUnfinishedRunAsync(run.Id);
                if (result.Success)
                {
                    _logger.LogInformation("Auto-resumed run {RunId} for Job '{JobName}' after key '{LastKey}'.", run.Id, run.JobName, checkpoint!.LastEvaluatedKey);
                    Notify(() => _events.RunRecovered(run, resumed: true));
                    continue;
                }
                if (result.Failure == TriggerFailure.AlreadyRunning || result.Failure == TriggerFailure.ShuttingDown)
                {
                    continue; // leave state untouched; picked up on a later start
                }
                _logger.LogInformation("Run {RunId} not auto-resumed: {Reason}", run.Id, result.Message);
                await _auditLogRepo.LogAsync("INFO", "CrashRecovery",
                    $"Run '{run.Id}' for Job '{run.JobName}' was not resumed automatically: {result.Message} It stays Interrupted with its checkpoint and is tried again at the next service start (or select it in Audit Runs & History and press Resume).",
                    scanRunId: run.Id, jobId: run.JobId, cancellationToken: cancellationToken);
            }

            if (run.Status != RunStatus.Interrupted)
            {
                run.Status = RunStatus.Interrupted;
                run.EndTimeUtc = DateTime.UtcNow;
                run.ErrorMessage = "Scan interrupted by service restart / system reboot. Recovery checkpoint recorded.";
                await _scanRunRepo.CompleteAsync(run, cancellationToken);
                // One event per run, when it is marked now; a run left Interrupted earlier is not announced again at every start.
                Notify(() => _events.RunRecovered(run, resumed: false));
            }
        }
    }

    private static void Notify(Action action)
    {
        try { action(); }
        catch { }
    }
}
