using System.Collections.Concurrent;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.IdleMon;

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
    public ScanRun Run { get; set; } = null!;
    public ComparisonJob Job { get; set; } = null!;
    /// <summary>When a cancel (or a service stop) was requested, or null. Set once.</summary>
    public DateTime? CancelRequestedUtc { get; set; }
    public bool CancelIsShutdown { get; set; }
    /// <summary>Completes when the engine task ended or the watchdog gave up on it.</summary>
    public TaskCompletionSource Settled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public bool Detached { get; set; }
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
    private readonly IIdleMonEvents _events;
    private readonly ScanOptions _scanOptions;
    private readonly INotificationQueue? _notifications;

    private readonly ConcurrentDictionary<string, ActiveScanContext> _activeScansByRunId = new();
    private readonly ConcurrentDictionary<string, string> _activeRunIdByJobId = new();
    private volatile bool _stopping;

    /// <summary>How long a cancelled scan may take to stop before the service reports it cancelled itself and detaches from it.</summary>
    public TimeSpan CancelWatchdogTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>Test hook: builds the engine of a scan (to inject fake S3 listers). Null = the normal engine.</summary>
    public Func<ComparisonJob, ScanRun, ScanCheckpoint?, ServiceComparisonEngine>? EngineFactory { get; set; }

    public ScanExecutionCoordinator(
        IJobRepository jobRepo,
        IScanRunRepository scanRunRepo,
        IDifferenceRepository diffRepo,
        IAuditLogRepository auditLogRepo,
        S3CompareService.Core.Notifications.IAlertNotificationService alertService,
        ILogger<ScanExecutionCoordinator> logger,
        IIdleMonEvents? events = null,
        ScanOptions? scanOptions = null,
        INotificationQueue? notifications = null)
    {
        _jobRepo = jobRepo;
        _scanRunRepo = scanRunRepo;
        _diffRepo = diffRepo;
        _auditLogRepo = auditLogRepo;
        _alertService = alertService;
        _logger = logger;
        _events = events ?? NoOpIdleMonEvents.Instance;
        _scanOptions = scanOptions ?? new ScanOptions();
        _notifications = notifications;
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
        // Service-wide defaults with the job's own throughput overrides (Performance) applied.
        var engine = EngineFactory?.Invoke(job, run, checkpoint)
            ?? new ServiceComparisonEngine(job, run, _scanRunRepo, _diffRepo, _auditLogRepo, checkpoint, options: _scanOptions.ForJob(job));

        var context = new ActiveScanContext
        {
            ScanRunId = run.Id,
            JobId = job.Id,
            JobName = job.Name,
            StartTimeUtc = run.StartTimeUtc,
            Engine = engine,
            Run = run,
            Job = job,
            Cts = cts,
            LatestStats = new ComparisonStatistics()
        };
        engine.ProgressUpdated += stats => context.LatestStats = stats;

        // The task waits for the gate so it can never finish (and clean up) before the context is published.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        context.ExecutionTask = Task.Run(async () =>
        {
            await gate.Task.ConfigureAwait(false);
            await ExecuteAsync(context, job, run, engine, cts, token).ConfigureAwait(false);
        });

        _activeScansByRunId[run.Id] = context;
        gate.SetResult();
    }

    private async Task ExecuteAsync(ActiveScanContext context, ComparisonJob job, ScanRun run, ServiceComparisonEngine engine, CancellationTokenSource cts, CancellationToken token)
    {
        bool completed = false;
        bool failed = false;
        try
        {
            _logger.LogInformation("Starting scan execution for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
            await engine.RunAsync(token);
            completed = !context.Detached;
            if (context.Detached) throw new OperationCanceledException(token);
            await _jobRepo.UpdateRunScheduleStateAsync(job.Id, DateTime.UtcNow, null, RunStatus.Completed);
            _logger.LogInformation("Scan execution finished for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
            Notify(() => _events.RunCompleted(job, run));
            await SafeAsync(() => LogStorageStatsAsync(job, run), "log difference storage");
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Only our own token counts as a cancel; an engine-internal timeout falls through to the Failed handler.
            if (context.Detached)
            {
                // The cancel watchdog already reported this run and cleared it; nothing more to record.
                _logger.LogWarning("Detached scan finally ended for Job {JobName} (RunId: {RunId}).", job.Name, run.Id);
            }
            else if (_stopping || run.Status == RunStatus.Interrupted)
            {
                // Host shutdown: the run stays Interrupted with its checkpoint and is resumed at next startup.
                // The job's last-run state is deliberately not advanced.
                _logger.LogWarning("Scan interrupted by service shutdown for Job '{JobName}' (RunId: {RunId}); it will resume at next start.", job.Name, run.Id);
            }
            else
            {
                _logger.LogWarning("Scan cancelled by user for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
                await SafeAsync(() => _jobRepo.UpdateRunScheduleStateAsync(job.Id, DateTime.UtcNow, null, RunStatus.Cancelled), "update job state");
                Notify(() => _events.RunCancelled(job, run));
            }
        }
        catch (Exception ex)
        {
            failed = true;
            _logger.LogError(ex, "Scan execution failed for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
            await SafeAsync(() => _jobRepo.UpdateRunScheduleStateAsync(job.Id, DateTime.UtcNow, null, RunStatus.Failed), "update job state");
            Notify(() => _events.RunFailed(job, run, ex));
        }

        // Post-scan alerting must never change the outcome of the scan itself.
        if (completed || failed)
        {
            try
            {
                var currentJob = await _jobRepo.GetByIdAsync(job.Id) ?? job;
                var (shouldAlert, reasons) = await _alertService.EvaluateAlertConditionAsync(currentJob, run);
                if (shouldAlert && _notifications != null)
                {
                    _notifications.EnqueueAlert(currentJob, run, reasons); // delivered on the dispatcher's task; the scan never waits for it
                }
                else if (shouldAlert)
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

        // Remove only our own entries: after a detach the job may already run again under another run id.
        _activeScansByRunId.TryRemove(run.Id, out _);
        _activeRunIdByJobId.TryRemove(new KeyValuePair<string, string>(job.Id, run.Id));
        context.Settled.TrySetResult();
        // Cancel/Stop callers tolerate ObjectDisposedException, so disposing here is safe.
        try { cts.Dispose(); } catch { }
    }

    // IdleMon events must never affect a scan: the implementations already swallow errors, this is the second belt.
    private static void Notify(Action action)
    {
        try { action(); }
        catch { }
    }

    /// <summary>Audit log line: how many of the run's differences were new records and how many were carried over unchanged.</summary>
    private async Task LogStorageStatsAsync(ComparisonJob job, ScanRun run)
    {
        var stats = await _diffRepo.GetStorageStatsAsync(run.Id).ConfigureAwait(false);
        if (stats == null || !stats.Deduplicated) return;
        await _auditLogRepo.LogAsync("INFO", "Storage",
            $"Run '{run.Id}' ({job.Name}): {stats.InRun:N0} stored difference(s); {stats.NewRecords:N0} new or changed (stored now), {stats.CarriedOver:N0} unchanged from the previous run (not stored again).",
            scanRunId: run.Id, jobId: job.Id).ConfigureAwait(false);
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
            BeginCancel(ctx, shutdown: false);
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

    /// <summary>
    /// Records that a cancel (or a service stop) was requested, writes it to the audit log at once, and starts the watchdog that
    /// reports the run itself when the engine does not stop in time. Only the first request counts.
    /// </summary>
    private void BeginCancel(ActiveScanContext ctx, bool shutdown)
    {
        lock (ctx)
        {
            if (ctx.CancelRequestedUtc != null) return;
            ctx.CancelRequestedUtc = DateTime.UtcNow;
            ctx.CancelIsShutdown = shutdown;
        }
        _ = AuditAsync("INFO", shutdown
            ? $"Service stop requested: interrupting run '{ctx.ScanRunId}' of job '{ctx.JobName}'."
            : $"Cancel requested for run '{ctx.ScanRunId}' of job '{ctx.JobName}'.", ctx, null);
        _ = Task.Run(() => WatchCancelAsync(ctx));
    }

    /// <summary>
    /// If the engine task has not ended <see cref="CancelWatchdogTimeout"/> after a cancel, something blocks that does not honour
    /// cancellation (a hung network call, a locked database). The service then logs what is stuck, marks the run Cancelled (or
    /// Interrupted on shutdown) itself, clears it from the active scans so the banner goes and the job can run again, and
    /// detaches the engine so that it cannot write to the run any more.
    /// </summary>
    private async Task WatchCancelAsync(ActiveScanContext ctx)
    {
        try
        {
            await Task.WhenAny(ctx.ExecutionTask, Task.Delay(CancelWatchdogTimeout)).ConfigureAwait(false);
            if (ctx.ExecutionTask.IsCompleted) return;

            bool shutdown = ctx.CancelIsShutdown;
            string stuck;
            try { stuck = string.Join("; ", ctx.Engine.GetInFlight().Select(o => o.ToString())); }
            catch { stuck = string.Empty; }
            if (stuck.Length == 0) stuck = "no S3 request or database call is in flight (the engine is blocked somewhere else)";
            ctx.Detached = true;
            ctx.Engine.Detach();
            _logger.LogError("Run {RunId} did not stop within {Seconds:0} s of the {What}; detaching. Stuck: {Stuck}", ctx.ScanRunId, CancelWatchdogTimeout.TotalSeconds, shutdown ? "service stop" : "cancel", stuck);
            await AuditAsync("ERROR",
                $"Run '{ctx.ScanRunId}' did not stop within {CancelWatchdogTimeout.TotalSeconds:0} s of the {(shutdown ? "service stop" : "cancel")} request. Stuck operation(s): {stuck}. "
                + $"The service marks the run {(shutdown ? "Interrupted" : "Cancelled")} itself and detaches from the scan; it can no longer change the run.", ctx, null).ConfigureAwait(false);

            ctx.Engine.SyncRunStats();
            var run = ctx.Run;
            run.Status = shutdown ? RunStatus.Interrupted : RunStatus.Cancelled;
            run.EndTimeUtc = DateTime.UtcNow;
            run.ErrorMessage = shutdown
                ? "Scan interrupted by service shutdown (the engine did not stop in time). Checkpoint saved for automatic recovery."
                : $"Scan was cancelled by user; the engine did not stop within {CancelWatchdogTimeout.TotalSeconds:0} s and was detached. Stuck: {stuck}";
            try { await _scanRunRepo.CompleteAsync(run, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false); }
            catch (Exception ex) { _logger.LogError(ex, "Could not store the final state of detached run {RunId}", ctx.ScanRunId); }

            if (!shutdown)
            {
                await SafeAsync(() => _jobRepo.UpdateRunScheduleStateAsync(ctx.JobId, DateTime.UtcNow, null, RunStatus.Cancelled).WaitAsync(TimeSpan.FromSeconds(15)), "update job state");
                Notify(() => _events.RunCancelled(ctx.Job, run));
            }

            _activeScansByRunId.TryRemove(ctx.ScanRunId, out _);
            _activeRunIdByJobId.TryRemove(new KeyValuePair<string, string>(ctx.JobId, ctx.ScanRunId));
            ctx.Settled.TrySetResult();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cancel watchdog for run {RunId} failed", ctx.ScanRunId);
            ctx.Settled.TrySetResult();
        }
    }

    /// <summary>Writes an audit log line without ever blocking or failing the caller for long.</summary>
    private async Task AuditAsync(string level, string message, ActiveScanContext ctx, string? details)
    {
        try
        {
            await _auditLogRepo.LogAsync(level, "ScanCoordinator", message, details: details, scanRunId: ctx.ScanRunId, jobId: ctx.JobId, cancellationToken: CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Audit log write failed or timed out: {Reason}", ex.Message);
        }
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
                BeginCancel(ctx, shutdown: true);
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

        // A scan the watchdog gave up on counts as settled, so a stuck scan cannot hold up the shutdown.
        var tasks = activeContexts.Select(c => c.Settled.Task).ToArray();
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
