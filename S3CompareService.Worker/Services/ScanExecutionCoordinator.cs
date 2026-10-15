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
    public Task ExecutionTask { get; set; } = null!;
}

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

    public IEnumerable<ActiveScanContext> GetActiveScans() => _activeScansByRunId.Values;

    public ActiveScanContext? GetActiveScan(string runId)
    {
        _activeScansByRunId.TryGetValue(runId, out var ctx);
        return ctx;
    }

    public bool IsJobRunning(string jobId) => _activeRunIdByJobId.ContainsKey(jobId);

    public async Task<(bool Success, string RunId, string Message)> TriggerScanAsync(string jobId, ScanCheckpoint? resumeCheckpoint = null)
    {
        var job = await _jobRepo.GetByIdAsync(jobId);
        if (job == null)
        {
            return (false, string.Empty, $"Job '{jobId}' not found.");
        }

        if (_activeRunIdByJobId.TryGetValue(jobId, out var existingRunId))
        {
            return (false, existingRunId, $"Job '{job.Name}' is already actively running (Run ID: {existingRunId}).");
        }

        var run = new ScanRun
        {
            Id = Guid.NewGuid().ToString("N"),
            JobId = job.Id,
            JobName = job.Name,
            StartTimeUtc = DateTime.UtcNow,
            Status = RunStatus.Running,
            Mode = job.Mode,
            ReplicationGracePeriodMinutes = job.ReplicationGracePeriodMinutes,
            IsResumedFromCheckpoint = resumeCheckpoint != null,
            ResumedFromKey = resumeCheckpoint?.LastEvaluatedKey
        };

        await _scanRunRepo.CreateAsync(run);

        var cts = new CancellationTokenSource();
        var engine = new ServiceComparisonEngine(
            job,
            run,
            _scanRunRepo,
            _diffRepo,
            _auditLogRepo,
            resumeCheckpoint);

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

        engine.ProgressUpdated += stats =>
        {
            context.LatestStats = stats;
        };

        _activeScansByRunId[run.Id] = context;
        _activeRunIdByJobId[job.Id] = run.Id;

        // Launch execution task
        context.ExecutionTask = Task.Run(async () =>
        {
            try
            {
                _logger.LogInformation("Starting scan execution for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
                await engine.RunAsync(cts.Token);
                await _jobRepo.UpdateRunScheduleStateAsync(job.Id, DateTime.UtcNow, null, RunStatus.Completed);
                _logger.LogInformation("Scan execution finished for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);

                // Evaluate Alert Thresholds & Dispatch Notifications
                var (shouldAlert, reasons) = await _alertService.EvaluateAlertConditionAsync(job, run);
                if (shouldAlert)
                {
                    await _alertService.DispatchAlertsAsync(job, run, reasons);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Scan execution cancelled for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
                await _jobRepo.UpdateRunScheduleStateAsync(job.Id, DateTime.UtcNow, null, RunStatus.Cancelled);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Scan execution failed for Job '{JobName}' (RunId: {RunId})", job.Name, run.Id);
                await _jobRepo.UpdateRunScheduleStateAsync(job.Id, DateTime.UtcNow, null, RunStatus.Failed);

                // Dispatch failure alert if configured
                var (shouldAlert, reasons) = await _alertService.EvaluateAlertConditionAsync(job, run);
                if (shouldAlert)
                {
                    await _alertService.DispatchAlertsAsync(job, run, reasons);
                }
            }
            finally
            {
                _activeScansByRunId.TryRemove(run.Id, out _);
                _activeRunIdByJobId.TryRemove(job.Id, out _);
                cts.Dispose();
            }
        });

        return (true, run.Id, $"Scan started for job '{job.Name}'.");
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
            ctx.Cts.Cancel();
            return true;
        }
        return false;
    }
}
