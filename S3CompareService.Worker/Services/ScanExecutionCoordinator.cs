using System.Collections.Concurrent;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Worker.Services;

public class ActiveScanContext
{
    public string ScanRunId { get; set; } = string.Empty;
    public string JobId { get; set; } = string.Empty;
    public string JobName { get; set; } = string.Empty;
    public DateTime StartTimeUtc { get; set; } = DateTime.UtcNow;
    public ServiceComparisonEngine Engine { get; set; } = null!;
    public CancellationTokenSource Cts { get; set; } = null!;
    public ComparisonStatistics LatestStats { get; set; } = new();
    public Task ExecutionTask { get; set; } = Task.CompletedTask;
}

public enum TriggerFailure
{
    None,
    NotFound,
    AlreadyRunning,
    Disabled,
    ShuttingDown,
    NotResumable
}

public readonly record struct TriggerResult(bool Success, string RunId, string Message, TriggerFailure Failure = TriggerFailure.None);

public class ScanExecutionCoordinator
{
    private readonly IJobRepository _jobRepo;
    private readonly IScanRunRepository _scanRunRepo;
    private readonly IDifferenceRepository _diffRepo;
    private readonly IAuditLogRepository _auditLogRepo;
    private readonly S3CompareService.Core.Notifications.IAlertNotificationService _alertService;
    private readonly ILogger<ScanExecutionCoordinator> _logger;

    private readonly ConcurrentDictionary<string, ActiveScanContext> _activeScansByRunId = new();
    private readonly ConcurrentDictionary<string, string> _activeRunIdByJobId = new();
    private volatile bool _stopping;

    public ScanExecutionCoordinator(
        IJobRepository jobRepo,
        IScanRunRepository scanRunRepo,
        IDifferenceRepository diffRepo,
        IAuditLogRepository auditLogRepo,
        S3CompareService.Core.Notifications.IAlertNotificationService alertService,
        ILogger<ScanExecutionCoordinator> logger)
    {
        _jobRepo = jobRepo;
        _scanRunRepo = scanRunRepo;
        _diffRepo = diffRepo;
        _auditLogRepo = auditLogRepo;
        _alertService = alertService;
        _logger = logger;
    }

    /// <summary>True once host shutdown has begun; no new scans may be started.</summary>
    public bool IsStopping => _stopping;

    /// <summary>Stops accepting new scans (manual, scheduled or recovery). Idempotent.</summary>
    public void BeginShutdown() => _stopping = true;

    public IEnumerable<ActiveScanContext> GetActiveScans() => _activeScansByRunId.Values;

    public ActiveScanContext? GetActiveScan(string runId)
    {
        _activeScansByRunId.TryGetValue(runId, out var ctx);
        return ctx;
    }

    public bool IsJobRunning(string jobId) => _activeRunIdByJobId.ContainsKey(jobId);

    public bool IsRunActive(string runId) => _activeScansByRunId.ContainsKey(runId);

    /// <summary>Starts a brand-new run for the job (job state is read fresh from the database).</summary>
    public async Task<TriggerResult> TriggerScanAsync(string jobId)
    {
        if (_stopping)
            return new(false, string.Empty, "The service is shutting down; new scans cannot be started.", TriggerFailure.ShuttingDown);

        var job = await _jobRepo.GetByIdAsync(jobId);
        if (job == null)
            return new(false, string.Empty, $"Job '{jobId}' not found.", TriggerFailure.NotFound);

        string runId = Guid.NewGuid().ToString("N");
        if (!_activeRunIdByJobId.TryAdd(job.Id, runId))
        {
            _activeRunIdByJobId.TryGetValue(job.Id, out var existingRunId);
            return new(false, existingRunId ?? string.Empty, $"Job '{job.Name}' is already actively running (Run ID: {existingRunId}).", TriggerFailure.AlreadyRunning);
        }

        try
        {
            var run = new ScanRun
            {
                Id = runId,
                JobId = job.Id,
                JobName = job.Name,
                StartTimeUtc = DateTime.UtcNow,
                Status = RunStatus.Running,
                Mode = job.Mode,
                ReplicationGracePeriodMinutes = job.ReplicationGracePeriodMinutes
            };

            await _scanRunRepo.CreateAsync(run);
            Launch(job, run, null);
            return new(true, run.Id, $"Scan started for job '{job.Name}'.");
        }
        catch
        {
            _activeRunIdByJobId.TryRemove(job.Id, out _);
            throw;
        }
    }

    /// <summary>
    /// Resumes an existing unfinished run IN PLACE (same run id): reopens it, loads its checkpoint and lets the engine
    /// continue. Used by crash recovery after a restart or a graceful shutdown.
    /// </summary>
    public async Task<TriggerResult> ResumeUnfinishedRunAsync(string runId)
    {
        if (_stopping)
            return new(false, runId, "The service is shutting down.", TriggerFailure.ShuttingDown);
        if (_activeScansByRunId.ContainsKey(runId))
            return new(false, runId, $"Run '{runId}' is already active in this process.", TriggerFailure.AlreadyRunning);

        var existing = await _scanRunRepo.GetByIdAsync(runId);
        if (existing == null)
            return new(false, runId, $"Run '{runId}' not found.", TriggerFailure.NotFound);

        var job = await _jobRepo.GetByIdAsync(existing.JobId);
        if (job == null)
            return new(false, runId, $"Job '{existing.JobId}' not found.", TriggerFailure.NotFound);
        if (!job.IsEnabled)
            return new(false, runId, $"Job '{job.Name}' is disabled.", TriggerFailure.Disabled);

        if (!_activeRunIdByJobId.TryAdd(job.Id, runId))
        {
            _activeRunIdByJobId.TryGetValue(job.Id, out var other);
            return new(false, other ?? string.Empty, $"Job '{job.Name}' is already actively running (Run ID: {other}).", TriggerFailure.AlreadyRunning);
        }

        try
        {
            var run = await _scanRunRepo.ReopenForResumeAsync(runId);
            if (run == null)
            {
                _activeRunIdByJobId.TryRemove(job.Id, out _);
                return new(false, runId, $"Run '{runId}' is not resumable.", TriggerFailure.NotResumable);
            }

            var checkpoint = await _scanRunRepo.GetCheckpointAsync(runId);
            Launch(job, run, checkpoint);
            return new(true, run.Id, $"Run '{run.Id}' for job '{job.Name}' resumed.");
        }
        catch
        {
            _activeRunIdByJobId.TryRemove(job.Id, out _);
            throw;
        }
    }

    /// <summary>Builds the context, publishes it atomically (with its ExecutionTask) and starts the scan.</summary>
    private void Launch(ComparisonJob job, ScanRun run, ScanCheckpoint? checkpoint)
    {
        var cts = new CancellationTokenSource();
        var token = cts.Token;
        var engine = new ServiceComparisonEngine(job, run, _scanRunRepo, _diffRepo, _auditLogRepo, checkpoint);

        var context = new ActiveScanContext
        {
            ScanRunId = run.Id,
            JobId = job.Id,
            JobName = job.Name,
            StartTimeUtc = run.StartTimeUtc,
            Engine = engine,
            Cts = cts,
            LatestStats = new ComparisonStatistics()
        };
        engine.ProgressUpdated += stats => context.LatestStats = stats;

        // The task waits for the gate so it can never finish (and clean up) before the context is published.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        context.ExecutionTask = Task.Run(async () =>
        {
            await gate.Task.ConfigureAwait(false);
            await ExecuteAsync(job, run, engine, cts, token).ConfigureAwait(false);
        });

        _activeScansByRunId[run.Id] = context;
        gate.SetResult();
    }

    private async Task ExecuteAsync(ComparisonJob job, ScanRun run, ServiceComparisonEngine engine, CancellationTokenSource cts, CancellationToken token)
    {
        bool completed = false;
        bool failed = false;
        try
        {
            _logger.LogInformation("Starting scan execution for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
            await engine.RunAsync(token);
            completed = true;
            await _jobRepo.UpdateRunScheduleStateAsync(job.Id, DateTime.UtcNow, null, RunStatus.Completed);
            _logger.LogInformation("Scan execution finished for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Only our own token counts as a cancel; an engine-internal timeout falls through to the Failed handler.
            if (_stopping || run.Status == RunStatus.Interrupted)
            {
                // Host shutdown: the run stays Interrupted with its checkpoint and is resumed at next startup.
                // The job's last-run state is deliberately not advanced.
                _logger.LogWarning("Scan interrupted by service shutdown for Job '{JobName}' (RunId: {RunId}); it will resume at next start.", job.Name, run.Id);
            }
            else
            {
                _logger.LogWarning("Scan cancelled by user for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
                await SafeAsync(() => _jobRepo.UpdateRunScheduleStateAsync(job.Id, DateTime.UtcNow, null, RunStatus.Cancelled), "update job state");
            }
        }
        catch (Exception ex)
        {
            failed = true;
            _logger.LogError(ex, "Scan execution failed for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
            await SafeAsync(() => _jobRepo.UpdateRunScheduleStateAsync(job.Id, DateTime.UtcNow, null, RunStatus.Failed), "update job state");
        }

        // Post-scan alerting must never change the outcome of the scan itself.
        if (completed || failed)
        {
            try
            {
                var currentJob = await _jobRepo.GetByIdAsync(job.Id) ?? job;
                var (shouldAlert, reasons) = await _alertService.EvaluateAlertConditionAsync(currentJob, run);
                if (shouldAlert)
                {
                    bool delivered = await _alertService.DispatchAlertsAsync(currentJob, run, reasons);
                    if (delivered)
                    {
                        await _jobRepo.UpdateAlertTimestampsAsync(job.Id, DateTime.UtcNow, null);
                    }
                }
            }
            catch (Exception alertEx)
            {
                _logger.LogError(alertEx, "Post-scan alert processing failed for Job '{JobName}' (RunId: {RunId}); the scan result is unaffected.", job.Name, run.Id);
            }
        }

        _activeScansByRunId.TryRemove(run.Id, out _);
        _activeRunIdByJobId.TryRemove(job.Id, out _);
        // Cancel/Stop callers tolerate ObjectDisposedException, so disposing here is safe.
        try { cts.Dispose(); } catch { }
    }

    private async Task SafeAsync(Func<Task> action, string what)
    {
        try { await action().ConfigureAwait(false); }
        catch (Exception ex) { _logger.LogError(ex, "Failed to {What}", what); }
    }

    public bool PauseScan(string runId)
    {
        if (_activeScansByRunId.TryGetValue(runId, out var ctx))
        {
            ctx.Engine.Pause();
            return true;
        }
        return false;
    }

    public bool ResumeScan(string runId)
    {
        if (_activeScansByRunId.TryGetValue(runId, out var ctx))
        {
            ctx.Engine.Resume();
            return true;
        }
        return false;
    }

    public bool CancelScan(string runId)
    {
        if (_activeScansByRunId.TryGetValue(runId, out var ctx))
        {
            try
            {
                ctx.Engine.Resume();
                ctx.Cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The scan finished between lookup and cancel.
            }
            return true;
        }
        return false;
    }

    public async Task StopAllActiveScansAsync(CancellationToken cancellationToken = default, TimeSpan? timeout = null)
    {
        BeginShutdown();

        var activeContexts = _activeScansByRunId.Values.ToList();
        if (activeContexts.Count == 0) return;

        _logger.LogInformation("Service shutting down: saving checkpoints and interrupting {Count} active scan(s)...", activeContexts.Count);
        foreach (var ctx in activeContexts)
        {
            try
            {
                ctx.Engine.MarkServiceShutdown();
                ctx.Cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to signal shutdown to run {RunId}", ctx.ScanRunId);
            }
        }

        var tasks = activeContexts.Select(c => c.ExecutionTask).ToArray();
        try
        {
            await Task.WhenAll(tasks).WaitAsync(timeout ?? TimeSpan.FromSeconds(45), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Not all scans finished checkpointing before the shutdown deadline: {Reason}", ex.Message);
        }
    }
}
