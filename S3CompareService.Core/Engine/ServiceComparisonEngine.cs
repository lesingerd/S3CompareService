using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Core.Engine;

/// <summary>
/// Streaming, checkpointed, parallel comparison of two S3 buckets.
///
/// The keyspace (keys relative to each bucket's prefix) is split into key ranges by <see cref="PartitionPlanner"/>; up to
/// <see cref="ScanOptions.MaxParallelPartitions"/> workers pull ranges from a queue and merge-join the two buckets' listings of
/// their range. Each listing is a <see cref="PrefetchingObjectStream"/> (background producer, bounded page buffer), so A and B
/// list concurrently and ahead of the comparison. With MaxParallelPartitions = 1 (or supplied test streams) there is one range.
///
/// Shared state (counters, the difference buffer, per-range progress) changes only under one lock, per key, in
/// <c>CommitKey</c>. Database writes go through a single-writer gate: one flush or checkpoint at a time. A checkpoint swaps the
/// difference buffer and snapshots every range's last completed key and the counters under that lock, inserts the swapped
/// differences, and only then saves the plan, so a persisted key never claims differences that are not stored.
///
/// Resume contract: pass the *same* ScanRun row (see <see cref="IScanRunRepository.ReopenForResumeAsync"/>) and its checkpoint
/// (<see cref="IScanRunRepository.GetCheckpointAsync"/>). The persisted plan is reused (no re-planning): completed ranges are
/// skipped, the others continue after their last completed key. A version 1 checkpoint (one key) resumes as one range. When
/// supplying streams instead of S3 (tests), they must already start after the checkpoint key, and only one range is possible.
/// </summary>
public class ServiceComparisonEngine
{
    private const int DifferenceBatchSize = 500;
    private const int MaxConsecutiveCheckpointFailures = 5;
    private const long CheckpointIntervalMs = 5000;
    private const long CheckpointEveryItems = 5000; // per worker, so many fast partitions do not checkpoint many times a second
    private const long ProgressIntervalMs = 250;
    /// <summary>Tries per list request of a scan (SDK retries are off for scan clients; see <see cref="S3Lister"/>).</summary>
    private const int ScanListAttempts = 3;

    private readonly ComparisonJob _job;
    private readonly ScanRun _scanRun;
    private readonly IScanRunRepository _scanRunRepo;
    private readonly IDifferenceRepository _diffRepo;
    private readonly IAuditLogRepository? _auditLogRepo;
    private readonly IS3ObjectStream? _providedStreamA;
    private readonly IS3ObjectStream? _providedStreamB;
    private readonly IS3Lister? _providedListerA;
    private readonly IS3Lister? _providedListerB;
    private readonly ScanCheckpoint? _resumeCheckpoint;
    private readonly List<PartitionRange>? _resumeRanges;
    private readonly bool _checkpointDiscarded;
    private readonly bool _replanAfterLegacyKey;
    private readonly ScanOptions _options;
    private readonly long _maxRecordedDifferences;

    private IAmazonS3? _clientA, _clientB;
    private IS3Lister? _listerA, _listerB;
    private IS3ObjectReader? _readerA, _readerB;
    private IRequestRateLimiter? _limiterA, _limiterB;
    private TokenBucketRateLimiter? _objectLimiter;
    private int _objectTokenBatch = 1;
    private readonly SemaphoreSlim _contentChecks;

    // ---- shared state: everything below is read and written under _stateLock ----
    private readonly object _stateLock = new();
    private readonly ComparisonStatistics _stats = new();
    private List<PartitionRange> _ranges = new() { new PartitionRange() };
    private List<RecordedDifference> _differenceBuffer = new();
    private long _recordedRows; // difference rows stored (or buffered) for this run; compared with the cap
    private readonly BucketAggregate _aggregateA = new();
    private readonly BucketAggregate _aggregateB = new();

    // ---- single writer: at most one flush or checkpoint at a time ----
    private readonly SemaphoreSlim _writerGate = new(1, 1);
    private long _lastBufferFlushTime;
    private long _lastCheckpointTime;
    private long _itemsSinceLastCheckpoint;
    private long _checkpointItemThreshold = CheckpointEveryItems;
    private long _lastProgressReportTime;
    private int _consecutiveCheckpointFailures;

    private int _handlerFailureLogged;
    private readonly Stopwatch _stopwatch = new();
    private readonly double _baselineElapsedSeconds;

    private readonly object _pauseLock = new();
    private volatile bool _isPaused = false;
    private volatile bool _isServiceShutdown = false;
    private TaskCompletionSource<bool>? _pauseTcs;
    private Task _statusPersistTask = Task.CompletedTask;

    // ---- diagnostics (live view for the UI; see ScanDiagnostics.cs) ----
    private const double SlowDbCallSeconds = 5;
    private const double SlowS3CallSeconds = 20;
    private readonly ScanDiagnostics _diag = new();
    private volatile bool _detached;
    private volatile PartitionPlanner? _planner;
    private PlannerStats? _finalPlannerStats;

    /// <summary>
    /// True once the host gave up on this scan (its cancel did not finish in time) and reported the run as cancelled itself.
    /// A detached engine writes nothing more to the database: no run state, no checkpoints, no differences, no log lines.
    /// </summary>
    public bool IsDetached => _detached;

    /// <summary>Copies the live counters into the run object (used by the host when it has to finish a stuck run itself).</summary>
    public void SyncRunStats() { try { UpdateLiveStats(); SyncStatsToRun(_scanRun, CloneStats()); } catch { } }

    /// <summary>Marks the engine detached (see <see cref="IsDetached"/>). Call it together with cancelling the scan.</summary>
    public void Detach()
    {
        _detached = true;
        _diag.Operation = "Detached by the service after a cancel timeout";
    }

    /// <summary>The operations (S3 requests, database calls) in flight right now, oldest first.</summary>
    public List<InFlightInfo> GetInFlight() => _diag.InFlight.Snapshot(8);

    private void SetPhase(ScanPhase phase, string? log = null)
    {
        var old = _diag.Phase;
        _diag.Phase = phase;
        if (old != phase && log != null) _ = LogAsync("INFO", log);
    }

    /// <summary>Names what the engine is doing and writes it to the audit log, so the log shows where a stuck scan stopped.</summary>
    private async Task StepAsync(string operation)
    {
        _diag.Operation = operation;
        await LogAsync("INFO", $"Scan step: {operation}").ConfigureAwait(false);
    }

    /// <summary>A snapshot of the scan for the banner and the diagnostics panel. Safe to call from any thread.</summary>
    public ScanDiagnosticsSnapshot GetDiagnostics(bool includePartitions = true)
    {
        var s = new ScanDiagnosticsSnapshot
        {
            RunId = _scanRun.Id,
            JobName = _job.Name,
            UnderlyingPhase = _diag.Phase.ToString(),
            IsPaused = _isPaused,
            Operation = _diag.Operation,
            ElapsedSeconds = ElapsedSeconds(),
            Planner = _planner?.GetStats() ?? _finalPlannerStats ?? new PlannerStats(),
            BucketA = _diag.BucketA.Snapshot(),
            BucketB = _diag.BucketB.Snapshot(),
            InFlight = _diag.InFlight.Snapshot(8),
            OldestS3 = _diag.InFlight.Oldest("S3"),
            OldestDb = _diag.InFlight.Oldest("DB"),
            LastError = _diag.LastError ?? _planner?.LastError ?? _finalPlannerStats?.LastError ?? (string.IsNullOrEmpty(_scanRun.ErrorMessage) ? null : _scanRun.ErrorMessage)
        };
        s.Phase = s.IsPaused ? "Paused" : s.UnderlyingPhase;

        var rows = new List<PartitionRow>();
        lock (_stateLock)
        {
            s.ObjectsCompared = _stats.ObjectsCompared;
            s.PartitionsTotal = _ranges.Count;
            s.PartitionsCompleted = _ranges.Count(r => r.Completed);
            s.PartitionsActive = _ranges.Count(r => r.Active && !r.Completed);
            s.PartitionsQueued = s.PartitionsTotal - s.PartitionsCompleted - s.PartitionsActive;
            if (includePartitions)
            {
                // All active ranges first, then the next queued ones, then the most recently finished, up to the cap; shown in key order.
                var picked = new HashSet<int>();
                void Take(IEnumerable<int> idx) { foreach (var i in idx) { if (picked.Count >= ScanDiagnosticsFormatter.MaxPartitionRows) return; picked.Add(i); } }
                var all = Enumerable.Range(0, _ranges.Count).ToList();
                Take(all.Where(i => _ranges[i].Active && !_ranges[i].Completed));
                Take(all.Where(i => !_ranges[i].Active && !_ranges[i].Completed));
                Take(Enumerable.Reverse(all).Where(i => _ranges[i].Completed));
                foreach (int i in picked.OrderBy(i => i))
                {
                    var r = _ranges[i];
                    rows.Add(new PartitionRow
                    {
                        Index = i,
                        Lower = r.Lower ?? "(start)",
                        Upper = r.Upper ?? "(end)",
                        State = r.Completed ? "done" : r.Active ? "active" : "queued",
                        LastCompletedKey = r.LastCompletedKey ?? string.Empty,
                        ObjectsCompared = r.ObjectsCompared
                    });
                }
                s.PartitionsOmitted = _ranges.Count - rows.Count;
            }
        }
        foreach (var r in rows)
        {
            r.Lower = InstrumentedLister.Clip(r.Lower);
            r.Upper = InstrumentedLister.Clip(r.Upper);
            r.LastCompletedKey = InstrumentedLister.Clip(r.LastCompletedKey);
        }
        s.Partitions = rows;
        s.PhaseText = ScanDiagnosticsFormatter.PhaseText(s);
        return s;
    }

    /// <summary>Every database call goes through here: tracked as in flight, bounded, abandoned on cancel, refused once detached, slow calls logged.</summary>
    private async Task<T> DbAsync<T>(string operation, Func<Task<T>> work, CancellationToken ct)
    {
        if (_detached) throw new OperationCanceledException("The run was detached after a cancel timeout; it writes nothing more.");
        using var scope = _diag.InFlight.Begin("DB", operation);
        long start = Stopwatch.GetTimestamp();
        try
        {
            // WaitAsync: a call that blocks or ignores its token must not hold up a cancel; it is abandoned after the timeout.
            return await work().WaitUnlessDoneAsync(_options.DbCallTimeout, ct).ConfigureAwait(false);
        }
        finally
        {
            double seconds = Stopwatch.GetElapsedTime(start).TotalSeconds;
            if (seconds >= SlowDbCallSeconds && Volatile.Read(ref scope.Op.Warned) == 0)
                _ = LogAsync("WARN", $"Database call '{operation}' took {seconds:0.0} s.");
        }
    }

    private async Task DbAsync(string operation, Func<Task> work, CancellationToken ct)
    {
        if (_detached) throw new OperationCanceledException("The run was detached after a cancel timeout; it writes nothing more.");
        using var scope = _diag.InFlight.Begin("DB", operation);
        long start = Stopwatch.GetTimestamp();
        try
        {
            await work().WaitUnlessDoneAsync(_options.DbCallTimeout, ct).ConfigureAwait(false);
        }
        finally
        {
            double seconds = Stopwatch.GetElapsedTime(start).TotalSeconds;
            if (seconds >= SlowDbCallSeconds && Volatile.Read(ref scope.Op.Warned) == 0)
                _ = LogAsync("WARN", $"Database call '{operation}' took {seconds:0.0} s.");
        }
    }

    /// <summary>
    /// Runs while the scan runs: raises progress about once a second (the workers only do so while comparing, so planning would
    /// otherwise be silent), and logs S3 requests and database calls that have been in flight too long, once each.
    /// </summary>
    private async Task MonitorAsync(CancellationToken ct)
    {
        try
        {
            using var timer = new PeriodicTimer(_options.MonitorInterval > TimeSpan.Zero ? _options.MonitorInterval : TimeSpan.FromSeconds(1));
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                UpdateLiveStats();
                RaiseProgress();
                foreach (var op in _diag.InFlight.TakeNewlyOverdue("DB", SlowDbCallSeconds))
                    _ = LogAsync("WARN", $"Database call '{op.Description}' has been running for {op.AgeSeconds:0} s and has not finished.");
                foreach (var op in _diag.InFlight.TakeNewlyOverdue("S3", SlowS3CallSeconds))
                    _ = LogAsync("WARN", $"S3 request has been in flight for {op.AgeSeconds:0} s (attempt {op.Attempt}): {op.Description}");
            }
        }
        catch (OperationCanceledException) { }
        catch { /* diagnostics must never break a scan */ }
    }


    /// <summary>
    /// Raised about every 250 ms from whichever worker is due (one at a time), and once at the end. Runs on a worker thread
    /// and holds up that worker while it runs, so keep it short. No engine lock is held while it runs.
    /// </summary>
    public event Action<ComparisonStatistics>? ProgressUpdated;

    /// <summary>
    /// Raised for every difference, on the worker thread that found it, with no engine lock held. With several partitions
    /// differences are found concurrently and not in key order, and the handler CAN be invoked concurrently from several
    /// workers: it must be thread-safe and fast (a slow handler slows the worker that raised it, not the others).
    /// </summary>
    public event Action<RecordedDifference>? DifferenceFound;

    /// <summary>Why a supplied checkpoint was ignored (job configuration changed), or null.</summary>
    public string? CheckpointDiscardReason { get; }

    /// <summary>True when the engine will continue from a valid checkpoint.</summary>
    public bool IsResuming => _resumeCheckpoint != null;

    /// <summary>The effective options of this scan (service defaults with the job's overrides already applied by the caller).</summary>
    public ScanOptions Options => _options;

    /// <summary>A copy of the current partition plan with its progress.</summary>
    public List<PartitionRange> GetPartitionsSnapshot()
    {
        lock (_stateLock) return _ranges.Select(r => r.Clone()).ToList();
    }

    /// <param name="options">The scan's effective options. Pass <c>serviceOptions.ForJob(job)</c> to honour per-job overrides.</param>
    /// <param name="streamA">Test hook: a ready listing of bucket A (single range; no planning).</param>
    /// <param name="streamB">Test hook: a ready listing of bucket B.</param>
    /// <param name="listerA">Test hook: list bucket A through this instead of an S3 client (partitioned path).</param>
    /// <param name="listerB">Test hook: list bucket B through this instead of an S3 client.</param>
    public ServiceComparisonEngine(
        ComparisonJob job,
        ScanRun scanRun,
        IScanRunRepository scanRunRepo,
        IDifferenceRepository diffRepo,
        IAuditLogRepository? auditLogRepo = null,
        ScanCheckpoint? resumeCheckpoint = null,
        IS3ObjectStream? streamA = null,
        IS3ObjectStream? streamB = null,
        ScanOptions? options = null,
        IS3Lister? listerA = null,
        IS3Lister? listerB = null)
    {
        _job = job ?? throw new ArgumentNullException(nameof(job));
        _scanRun = scanRun ?? throw new ArgumentNullException(nameof(scanRun));
        _scanRunRepo = scanRunRepo ?? throw new ArgumentNullException(nameof(scanRunRepo));
        _diffRepo = diffRepo ?? throw new ArgumentNullException(nameof(diffRepo));
        _auditLogRepo = auditLogRepo;
        _providedStreamA = streamA;
        _providedStreamB = streamB;
        _providedListerA = listerA;
        _providedListerB = listerB;
        _options = options?.Clone() ?? new ScanOptions();
        _options.ApplyMemoryCap();
        _maxRecordedDifferences = _options.MaxRecordedDifferencesPerRun;
        _contentChecks = new SemaphoreSlim(Math.Max(1, _options.MaxConcurrentContentChecks));

        if (resumeCheckpoint != null)
        {
            var reason = ValidateCheckpoint(resumeCheckpoint);
            List<PartitionRange>? ranges = null;
            if (reason == null)
            {
                ranges = resumeCheckpoint.Partitions is { Count: > 0 } plan
                    ? plan.Select(r => r.Clone()).ToList()
                    : new List<PartitionRange> { new() { LastCompletedKey = resumeCheckpoint.HasKey ? resumeCheckpoint.LastEvaluatedKey ?? string.Empty : null } };
                var tiling = PartitionPlanner.ValidateTiling(ranges);
                if (tiling != null) reason = $"partition plan is invalid ({tiling})";
                else if (ranges.Count > 1 && UsesProvidedStreams) reason = "a partitioned checkpoint cannot resume over supplied streams";
            }

            CheckpointDiscardReason = reason;
            if (reason == null)
            {
                _resumeCheckpoint = resumeCheckpoint;
                _resumeRanges = ranges;
                _ranges = ranges!.Select(r => r.Clone()).ToList();
                // A version 1 checkpoint has one key and no plan: the rest of the keyspace is planned (and scanned in
                // parallel) after that key at the start of RunAsync.
                _replanAfterLegacyKey = resumeCheckpoint.Partitions is not { Count: > 0 } && !UsesProvidedStreams;
                _baselineElapsedSeconds = resumeCheckpoint.ElapsedSeconds;
                InitializeStatsFromCheckpoint(resumeCheckpoint);
                _scanRun.IsResumedFromCheckpoint = true;
                _scanRun.ResumedFromKey ??= resumeCheckpoint.LastEvaluatedKey;
            }
            else
            {
                _checkpointDiscarded = true;
            }
        }
    }

    private bool UsesProvidedStreams => _providedStreamA != null && _providedStreamB != null;

    private string? ValidateCheckpoint(ScanCheckpoint cp)
    {
        if (!cp.CanResume) return "checkpoint has no evaluated key";
        if (!string.IsNullOrEmpty(cp.JobId) && !string.Equals(cp.JobId, _job.Id, StringComparison.Ordinal))
            return $"checkpoint belongs to job '{cp.JobId}', not '{_job.Id}'";
        if (!string.IsNullOrEmpty(cp.BucketNameA) && cp.BucketNameA != _job.BucketA.BucketName)
            return "bucket A name changed";
        if (!string.IsNullOrEmpty(cp.BucketNameB) && cp.BucketNameB != _job.BucketB.BucketName)
            return "bucket B name changed";
        if (!string.IsNullOrEmpty(cp.EndpointA) && cp.EndpointA != _job.BucketA.EndpointUrl)
            return "bucket A endpoint changed";
        if (!string.IsNullOrEmpty(cp.EndpointB) && cp.EndpointB != _job.BucketB.EndpointUrl)
            return "bucket B endpoint changed";
        if ((cp.PrefixA ?? string.Empty) != (_job.BucketA.Prefix ?? string.Empty))
            return "bucket A prefix changed";
        if ((cp.PrefixB ?? string.Empty) != (_job.BucketB.Prefix ?? string.Empty))
            return "bucket B prefix changed";
        if (cp.Mode != _job.Mode)
            return "comparison mode changed";
        return null;
    }

    private void InitializeStatsFromCheckpoint(ScanCheckpoint cp)
    {
        _stats.ObjectsCompared = cp.ObjectsCompared;
        _stats.MatchedCount = cp.MatchedCount;
        _stats.TotalDifferences = cp.TotalDifferences;
        _stats.FilteredDifferencesCount = cp.FilteredDifferencesCount;
        _stats.InFlightDifferencesCount = cp.InFlightDifferencesCount;
        _stats.MissingInBucketBCount = cp.MissingInBucketBCount;
        _stats.MissingInBucketACount = cp.MissingInBucketACount;
        _stats.SizeMismatchCount = cp.SizeMismatchCount;
        _stats.ETagMismatchCount = cp.ETagMismatchCount;
        _stats.ContentMismatchCount = cp.ContentMismatchCount;
        _stats.SequenceGapCount = cp.SequenceGapCount;
        _stats.ContentCheckErrorCount = cp.ContentCheckErrorCount;
        _stats.BucketAObjectsRead = cp.BucketAObjectsRead;
        _stats.BucketBObjectsRead = cp.BucketBObjectsRead;
        _stats.BucketABytesRead = cp.BucketABytesRead;
        _stats.BucketBBytesRead = cp.BucketBBytesRead;
        _stats.ElapsedTime = TimeSpan.FromSeconds(cp.ElapsedSeconds);
        _recordedRows = _maxRecordedDifferences > 0 ? Math.Min(cp.TotalDifferences, _maxRecordedDifferences) : cp.TotalDifferences;
    }

    public void Pause()
    {
        lock (_pauseLock)
        {
            if (!_isPaused)
            {
                _isPaused = true;
                _stopwatch.Stop();
                _pauseTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                _scanRun.Status = RunStatus.Paused;
                QueueStatusPersist();
            }
        }
    }

    public void Resume()
    {
        lock (_pauseLock)
        {
            if (_isPaused)
            {
                _isPaused = false;
                _stopwatch.Start();
                _scanRun.Status = RunStatus.Running;
                _pauseTcs?.TrySetResult(true);
                _pauseTcs = null;
                QueueStatusPersist();
            }
        }
    }

    // Status writes triggered by Pause/Resume are chained so they execute in order, their failures are observed,
    // and RunAsync can await them before writing the final state (a late "Running" must not overwrite "Completed").
    private void QueueStatusPersist()
    {
        _statusPersistTask = _statusPersistTask.ContinueWith(async _ =>
        {
            try
            {
                await DbAsync("UpdateProgressAsync", () => _scanRunRepo.UpdateProgressAsync(_scanRun, CancellationToken.None), CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await LogAsync("WARN", $"Failed to persist run status '{_scanRun.Status}': {ex.Message}").ConfigureAwait(false);
            }
        }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default).Unwrap();
    }

    public void MarkServiceShutdown()
    {
        _isServiceShutdown = true;
        Resume();
    }

    public bool IsPaused => _isPaused;

    private static string GetRelativeKey(string fullKey, string? prefix)
    {
        if (string.IsNullOrEmpty(prefix))
            return fullKey;
        return fullKey.StartsWith(prefix, StringComparison.Ordinal)
            ? fullKey.Substring(prefix.Length)
            : fullKey;
    }

    /// <summary>Checkpoint keys are always relative to the bucket prefix, so the S3 start-after key is prefix + relative key.</summary>
    public static string? BuildStartAfterKey(bool hasKey, string? relativeKey, string? prefix)
    {
        if (!hasKey) return null;
        string full = (prefix ?? string.Empty) + (relativeKey ?? string.Empty);
        return full.Length == 0 ? null : full;
    }

    private async Task LogAsync(string level, string message, string? details = null)
    {
        if (_auditLogRepo == null || _detached) return;
        try
        {
            // Tracked and bounded: a blocked database must not hang the scan through its own log lines.
            using var scope = _diag.InFlight.Begin("DB", "Audit log write");
            await _auditLogRepo.LogAsync(level, "ScanEngine", message, details: details, scanRunId: _scanRun.Id, jobId: _job.Id, cancellationToken: CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        }
        catch
        {
            // audit logging must never break a scan
        }
    }

    private void ApplyGraceWindow()
    {
        if (_job.ReplicationGracePeriodMinutes > 0)
        {
            _scanRun.ReplicationGracePeriodMinutes = _job.ReplicationGracePeriodMinutes;
            _scanRun.GracePeriodCutoffUtc = _scanRun.StartTimeUtc.AddMinutes(-_job.ReplicationGracePeriodMinutes);
            _scanRun.IsScanWindowClipped = true;
            _scanRun.ScanWindowNotes = $"Replication lag grace window active: objects modified after {_scanRun.GracePeriodCutoffUtc:yyyy-MM-dd HH:mm:ss} UTC ({_job.ReplicationGracePeriodMinutes} min) are flagged as in-flight replication.";
        }
        else
        {
            _scanRun.ReplicationGracePeriodMinutes = 0;
            _scanRun.GracePeriodCutoffUtc = null;
            _scanRun.IsScanWindowClipped = false;
            _scanRun.ScanWindowNotes = "Full scan window (no grace period exclusion).";
        }

        // A resumed run that was already truncated keeps saying so.
        if (_scanRun.DifferencesTruncated) AppendTruncationNote();
    }

    private void AppendTruncationNote()
    {
        _scanRun.ScanWindowNotes = (_scanRun.ScanWindowNotes + " " + TruncationNote(_maxRecordedDifferences)).Trim();
    }

    /// <summary>The text recorded in <see cref="ScanRun.ScanWindowNotes"/> (and shown in exports) when the difference cap was hit.</summary>
    public static string TruncationNote(long cap) =>
        $"Differences truncated: only the first {cap:N0} differences were stored; the counters below still count all of them.";

    /// <summary>The frontier key of the current plan (see <see cref="PartitionPlanner.Frontier"/>). Caller holds no lock.</summary>
    private string FrontierKey()
    {
        lock (_stateLock) return PartitionPlanner.Frontier(_ranges).Key;
    }

    public async Task<ScanRun> RunAsync(CancellationToken cancellationToken = default)
    {
        _stopwatch.Restart();
        _scanRun.Status = RunStatus.Running;

        // Resuming into the same run keeps the original start time (and therefore the same grace cutoff) so
        // differences classified before and after the interruption are consistent.
        bool resumingSameRun = _resumeCheckpoint != null && _resumeCheckpoint.ScanRunId == _scanRun.Id;
        if (!resumingSameRun)
        {
            _scanRun.StartTimeUtc = DateTime.UtcNow;
        }
        ApplyGraceWindow();

        string? prefixA = string.IsNullOrEmpty(_job.BucketA.Prefix) ? null : _job.BucketA.Prefix;
        string? prefixB = string.IsNullOrEmpty(_job.BucketB.Prefix) ? null : _job.BucketB.Prefix;
        _diag.ConfigureBuckets(_job.BucketA.BucketName, _job.BucketB.BucketName, _options.MaxListRequestsPerSecond);
        _diag.Phase = ScanPhase.Preparing;
        _diag.Operation = "Starting";
        using var monitorCts = new CancellationTokenSource();
        var monitorTask = Task.Run(() => MonitorAsync(monitorCts.Token));

        try
        {
            await StepAsync("Preparing run state").ConfigureAwait(false);
            await PrepareRunStateAsync(cancellationToken).ConfigureAwait(false);

            await DbAsync("UpdateProgressAsync", () => _scanRunRepo.UpdateProgressAsync(_scanRun, cancellationToken), cancellationToken).ConfigureAwait(false);
            if (_checkpointDiscarded)
            {
                await LogAsync("WARN", $"Checkpoint for run '{_scanRun.Id}' was discarded ({CheckpointDiscardReason}); starting a fresh scan.").ConfigureAwait(false);
            }
            await LogAsync("INFO", $"Starting scan run '{_scanRun.Id}' for job '{_job.Name}' (Grace Period: {_job.ReplicationGracePeriodMinutes}m){(IsResuming ? $", resuming after key '{FrontierKey()}'" : string.Empty)}").ConfigureAwait(false);

            if (_options.MaxObjectsPerSecond > 0)
            {
                // Paid in batches of about 1/20 s worth of keys, so the limiter costs one lock per batch, not per key.
                _objectTokenBatch = (int)Math.Clamp(_options.MaxObjectsPerSecond / 20, 1, 256);
                _objectLimiter = new TokenBucketRateLimiter(_options.MaxObjectsPerSecond, burst: Math.Max(_options.MaxObjectsPerSecond, _objectTokenBatch));
            }

            if (!UsesProvidedStreams)
            {
                await StepAsync("Creating S3 clients").ConfigureAwait(false);
                int connections = S3ClientFactory.ConnectionsForScan(_options.MaxParallelPartitions, _job.Mode == ComparisonMode.FullData ? _options.MaxConcurrentContentChecks : 0);
                _listerA = _providedListerA;
                _listerB = _providedListerB;
                if (_listerA == null)
                {
                    _clientA = S3ClientFactory.CreateClient(_job.BucketA, connections, maxErrorRetry: 0);
                    _listerA = new S3Lister(_clientA, maxAttempts: ScanListAttempts);
                }
                if (_listerB == null)
                {
                    _clientB = S3ClientFactory.CreateClient(_job.BucketB, connections, maxErrorRetry: 0);
                    _listerB = new S3Lister(_clientB, maxAttempts: ScanListAttempts);
                }
                // One limiter per bucket side for the whole scan: every partition, the planner and content reads share it.
                _readerA = _listerA as IS3ObjectReader;
                _readerB = _listerB as IS3ObjectReader;
                _limiterA = TokenBucketRateLimiter.Create(_options.MaxListRequestsPerSecond);
                _limiterB = TokenBucketRateLimiter.Create(_options.MaxListRequestsPerSecond);
                // Every list request and every wait in the limiter is counted and timed for the diagnostics panel.
                _listerA = new InstrumentedLister(_listerA, _diag.BucketA, _diag.InFlight);
                _listerB = new InstrumentedLister(_listerB, _diag.BucketB, _diag.InFlight);
                if (_limiterA != null) _limiterA = new InstrumentedLimiter(_limiterA, _diag.BucketA);
                if (_limiterB != null) _limiterB = new InstrumentedLimiter(_limiterB, _diag.BucketB);

                if (_resumeRanges == null)
                {
                    var plan = await PlanPartitionsAsync(prefixA, prefixB, cancellationToken).ConfigureAwait(false);
                    lock (_stateLock) _ranges = plan;
                }
                else if (_replanAfterLegacyKey)
                {
                    string done = _resumeRanges[0].LastCompletedKey ?? string.Empty;
                    var plan = PartitionPlanner.ResumePlanAfter(await PlanPartitionsAsync(prefixA, prefixB, cancellationToken).ConfigureAwait(false), done);
                    lock (_stateLock) _ranges = plan;
                    await LogAsync("INFO", $"Resume from a single-key checkpoint: the keyspace after '{done}' was planned as {plan.Count - 1} key range(s).").ConfigureAwait(false);
                }
            }

            lock (_stateLock)
            {
                _stats.PartitionsTotal = _ranges.Count;
                _stats.PartitionsCompleted = _ranges.Count(r => r.Completed);
            }

            SetPhase(ScanPhase.Scanning, $"Scan phase: Scanning ({_stats.PartitionsTotal} key range(s), {_stats.PartitionsCompleted} already done, up to {Math.Min(_options.MaxParallelPartitions, Math.Max(1, _stats.PartitionsTotal - _stats.PartitionsCompleted))} worker(s)).");

            await RunPartitionsAsync(prefixA, prefixB, cancellationToken).ConfigureAwait(false);

            SetPhase(ScanPhase.Finalizing, "Scan phase: Finalizing (all key ranges compared; writing the final results).");
            _diag.Operation = "Finalizing: flushing differences and metadata";

            // Flush any remaining differences
            await FlushDifferencesAsync(cancellationToken).ConfigureAwait(false);

            // Record Bucket Metadata Snapshots
            if (UsesProvidedStreams)
            {
                lock (_stateLock)
                {
                    _aggregateA.Add(_providedStreamA!);
                    _aggregateB.Add(_providedStreamB!);
                }
            }
            await RecordBucketMetadataSnapshotsAsync(cancellationToken).ConfigureAwait(false);

            // Scan completed normally
            UpdateLiveStats(includeSystemDiagnostics: true);
            _scanRun.Status = RunStatus.Completed;
            _scanRun.EndTimeUtc = DateTime.UtcNow;
            _scanRun.ErrorMessage = null;
            _scanRun.LastEvaluatedKey = FrontierKey();
            SyncStatsToRun(_scanRun, CloneStats());

            await ObserveStatusPersistAsync().ConfigureAwait(false);
            await DbAsync("CompleteAsync", () => _scanRunRepo.CompleteAsync(_scanRun, cancellationToken), cancellationToken).ConfigureAwait(false);
            await DbAsync("DeleteCheckpointAsync", () => _scanRunRepo.DeleteCheckpointAsync(_scanRun.Id, cancellationToken), cancellationToken).ConfigureAwait(false);

            await LogAsync("INFO", $"Scan run '{_scanRun.Id}' completed successfully: {_scanRun.ObjectsCompared:N0} objects evaluated, {_scanRun.MatchedCount:N0} matched, {_scanRun.TotalDifferences:N0} raw diffs ({_scanRun.FilteredDifferencesCount:N0} confirmed disparities, {_scanRun.InFlightDifferencesCount:N0} in-flight replication).").ConfigureAwait(false);
        }
        catch (Exception ex) when (cancellationToken.IsCancellationRequested)
        {
            // Cancelled (user) or interrupted (shutdown). A worker or the final steps may surface the cancellation as some
            // other exception (an SDK or I/O error from an aborted request); that is still a cancellation, not a failure.
            var status = _isServiceShutdown ? RunStatus.Interrupted : RunStatus.Cancelled;
            string message = _isServiceShutdown
                ? "Scan interrupted by service shutdown. Checkpoint saved for automatic recovery."
                : "Scan was cancelled by user.";
            await FinalizeRunAsync(status, message).ConfigureAwait(false);

            await LogAsync(
                _isServiceShutdown ? "RECOVERY" : "WARN",
                $"Scan run '{_scanRun.Id}' {(_isServiceShutdown ? "interrupted for service shutdown" : "cancelled")}. Checkpoint saved at key '{FrontierKey()}'.",
                ex is OperationCanceledException ? null : $"Surfaced as {ex.GetType().Name}: {ex.Message}").ConfigureAwait(false);
            if (ex is OperationCanceledException) throw;
            throw new OperationCanceledException(message, ex, cancellationToken);
        }
        catch (Exception ex)
        {
            _diag.RecordError($"Run failed: {ex.GetType().Name}: {ex.Message}");
            await FinalizeRunAsync(RunStatus.Failed, $"{ex.GetType().Name}: {ex.Message}").ConfigureAwait(false);
            await LogAsync("ERROR", $"Scan run '{_scanRun.Id}' failed with error: {ex.Message}", ex.StackTrace).ConfigureAwait(false);
            throw;
        }
        finally
        {
            _stopwatch.Stop();
            try { monitorCts.Cancel(); await monitorTask.ConfigureAwait(false); } catch { }
            try { if (_providedStreamA != null) await _providedStreamA.DisposeAsync().ConfigureAwait(false); } catch { }
            try { if (_providedStreamB != null) await _providedStreamB.DisposeAsync().ConfigureAwait(false); } catch { }
            try { _clientA?.Dispose(); } catch { }
            try { _clientB?.Dispose(); } catch { }
        }

        UpdateLiveStats(includeSystemDiagnostics: true);
        RaiseProgress();
        return _scanRun;
    }

    /// <summary>Plans the key ranges. A planning failure is not fatal: the scan then runs as one range.</summary>
    private async Task<List<PartitionRange>> PlanPartitionsAsync(string? prefixA, string? prefixB, CancellationToken ct)
    {
        if (_options.MaxParallelPartitions <= 1 || _options.MaxDiscoveryRequests <= 0)
        {
            await LogAsync("INFO", _options.MaxParallelPartitions <= 1
                ? "Planning skipped: MaxParallelPartitions is 1, so the scan runs as one key range."
                : "Planning skipped: MaxDiscoveryRequests is 0, so the scan runs as one key range.").ConfigureAwait(false);
            return PartitionPlanner.BuildRanges(Array.Empty<string>(), 1);
        }

        var limit = _options.MaxPlanningSeconds > 0 ? TimeSpan.FromSeconds(_options.MaxPlanningSeconds) : (TimeSpan?)null;
        var planner = new PartitionPlanner(
            _listerA!, _job.BucketA.BucketName, prefixA, _limiterA,
            _listerB!, _job.BucketB.BucketName, prefixB, _limiterB,
            _options.TargetPartitions, _options.MaxDiscoveryDepth, _options.MaxDiscoveryRequests,
            pauseGate: CheckPauseAsync,
            maxPlanningTime: limit,
            requestTimeout: _options.PlannerRequestTimeout,
            log: (level, message) => _ = LogAsync(level, message));
        _planner = planner;
        SetPhase(ScanPhase.Planning);
        _diag.Operation = "Planning key ranges";
        await LogAsync("INFO",
            $"Planning started for run '{_scanRun.Id}': request budget {_options.MaxDiscoveryRequests}, target {_options.TargetPartitions} key range(s) for {_options.MaxParallelPartitions} parallel worker(s), "
            + $"rate limit {(_options.MaxListRequestsPerSecond > 0 ? _options.MaxListRequestsPerSecond.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture) + " requests/s per bucket" : "unlimited")}, "
            + $"time limit {(limit == null ? "none" : _options.MaxPlanningSeconds + " s")}, max depth {_options.MaxDiscoveryDepth}, "
            + $"prefix A '{prefixA ?? string.Empty}', prefix B '{prefixB ?? string.Empty}', per-request timeout {PartitionPlanner.DefaultRequestTimeout.TotalSeconds:0} s.").ConfigureAwait(false);
        var sw = Stopwatch.StartNew();
        try
        {
            var plan = await planner.PlanAsync(ct).ConfigureAwait(false);
            await LogAsync("INFO", $"Partition plan for run '{_scanRun.Id}': {plan.Count} key range(s) for {_options.MaxParallelPartitions} parallel worker(s) (method: {planner.Method}, {planner.CandidateCount} candidate boundaries, {planner.RequestsUsed} planning requests, {planner.RequestErrors} request error(s){(planner.TimedOut ? ", time limit reached" : string.Empty)}{(planner.JumpDisabled ? ", shared-prefix jump disabled" : string.Empty)}, {sw.ElapsedMilliseconds} ms).").ConfigureAwait(false);
            return plan;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _diag.RecordError($"Planning failed: {ex.Message}");
            await LogAsync("WARN", $"Partition planning failed for run '{_scanRun.Id}' ({ex.Message}); scanning as one key range.", ex.StackTrace).ConfigureAwait(false);
            return PartitionPlanner.BuildRanges(Array.Empty<string>(), 1);
        }
        finally
        {
            _finalPlannerStats = planner.GetStats();
            _planner = null;
        }
    }

    /// <summary>Runs the workers over all unfinished ranges. All workers stop when one fails or the scan is cancelled.</summary>
    private async Task RunPartitionsAsync(string? prefixA, string? prefixB, CancellationToken ct)
    {
        List<PartitionRange> pending;
        lock (_stateLock) pending = _ranges.Where(r => !r.Completed).ToList();
        if (pending.Count == 0) return;

        var queue = new ConcurrentQueue<PartitionRange>(pending);
        int workerCount = UsesProvidedStreams ? 1 : Math.Clamp(_options.MaxParallelPartitions, 1, pending.Count);
        _checkpointItemThreshold = CheckpointEveryItems * workerCount;
        _diag.Operation = $"Workers running ({workerCount})";
        await LogAsync("INFO", $"Scan step: Workers started ({workerCount} worker(s) for {pending.Count} unfinished key range(s)).").ConfigureAwait(false);
        using var workersCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var errors = new ConcurrentQueue<Exception>();

        async Task WorkerAsync()
        {
            // Object-rate tokens are owed per worker, across its ranges: ranges smaller than a token batch still pay.
            var tokens = new ObjectTokenAccount();
            try
            {
                while (queue.TryDequeue(out var range))
                {
                    await RunRangeAsync(range, prefixA, prefixB, tokens, workersCts.Token).ConfigureAwait(false);
                }
                await PayObjectTokensAsync(tokens, workersCts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                errors.Enqueue(ex);
                try { workersCts.Cancel(); } catch (ObjectDisposedException) { }
            }
        }

        if (workerCount == 1)
        {
            await WorkerAsync().ConfigureAwait(false);
        }
        else
        {
            await Task.WhenAll(Enumerable.Range(0, workerCount).Select(_ => Task.Run(WorkerAsync))).ConfigureAwait(false);
        }

        if (!errors.IsEmpty)
        {
            // The caller's cancellation wins (other workers then only saw its echo); otherwise report the root cause.
            ct.ThrowIfCancellationRequested();
            var root = errors.FirstOrDefault(e => e is not OperationCanceledException) ?? errors.First();
            ExceptionDispatchInfo.Capture(root).Throw();
        }
    }

    /// <summary>Per-worker merge state of one range.</summary>
    private sealed class RangeCursor
    {
        public required PartitionRange Range { get; init; }
        public SequenceGapDetector? GapDetector { get; init; }
        public required ObjectTokenAccount Tokens { get; init; }
    }

    /// <summary>Compared keys a worker has not yet paid to the object-rate limiter.</summary>
    private sealed class ObjectTokenAccount
    {
        public int Pending;
    }

    /// <summary>Merge-joins the two listings of one key range.</summary>
    private async Task RunRangeAsync(PartitionRange range, string? prefixA, string? prefixB, ObjectTokenAccount tokens, CancellationToken ct)
    {
        IS3ObjectStream enumA, enumB;
        bool ownsStreams = !UsesProvidedStreams;
        string? after;
        lock (_stateLock) after = range.ResumeAfter;
        if (ownsStreams)
        {
            bool hasAfter = after != null;
            enumA = new PrefetchingObjectStream(_listerA!, _job.BucketA.BucketName, prefixA, BuildStartAfterKey(hasAfter, after, prefixA), range.Upper, _options.PrefetchPages, _limiterA, _options.ListPageSize, ct);
            enumB = new PrefetchingObjectStream(_listerB!, _job.BucketB.BucketName, prefixB, BuildStartAfterKey(hasAfter, after, prefixB), range.Upper, _options.PrefetchPages, _limiterB, _options.ListPageSize, ct);
        }
        else
        {
            enumA = _providedStreamA!;
            enumB = _providedStreamB!;
        }

        // Each range has its own gap detector, seeded with the key just before the range (its lower boundary, or the last key
        // done when resuming), so a numeric series that crosses the boundary is checked as in one sequential listing.
        var cursor = new RangeCursor { Range = range, Tokens = tokens, GapDetector = _job.DetectSequenceGaps ? new SequenceGapDetector(_job.MaxSequenceGapThreshold) : null };
        if (cursor.GapDetector != null && !string.IsNullOrEmpty(after))
        {
            long unusedIdCounter = 0;
            cursor.GapDetector.CheckKey(after, _scanRun.Id, ref unusedIdCounter); // primes the series; reports nothing
        }
        lock (_stateLock) { _stats.PartitionsActive++; range.Active = true; }
        try
        {
            bool hasA = await enumA.MoveNextAsync(ct).ConfigureAwait(false);
            bool hasB = await enumB.MoveNextAsync(ct).ConfigureAwait(false);

            while (hasA || hasB)
            {
                await CheckPauseAsync(ct).ConfigureAwait(false);
                ct.ThrowIfCancellationRequested();
                await ThrottleObjectsAsync(cursor, ct).ConfigureAwait(false);

                S3ObjectSummary? itemA = hasA ? enumA.Current : null;
                S3ObjectSummary? itemB = hasB ? enumB.Current : null;
                string? relKeyA = itemA != null ? GetRelativeKey(itemA.Key, prefixA) : null;
                string? relKeyB = itemB != null ? GetRelativeKey(itemB.Key, prefixB) : null;

                int cmp;
                if (itemA == null) cmp = 1;
                else if (itemB == null) cmp = -1;
                else cmp = Utf8OrdinalComparer.CompareStatic(relKeyA, relKeyB);

                // Phase 1: everything that can be slow or cancelled (S3 data compare) - mutates no state.
                // Phase 2 (CommitKey): synchronous bookkeeping - the key is either fully recorded or not at all.
                if (cmp == 0)
                {
                    var (matched, diff) = await CompareSameKeyAsync(relKeyA!, itemA!, itemB!, ct).ConfigureAwait(false);
                    CommitKey(cursor, relKeyA!, itemA, itemB, matched, diff);
                    hasA = await enumA.MoveNextAsync(ct).ConfigureAwait(false);
                    hasB = await enumB.MoveNextAsync(ct).ConfigureAwait(false);
                }
                else if (cmp < 0)
                {
                    bool tail = itemB == null;
                    var diff = MakeDifference(
                        relKeyA!,
                        DifferenceType.MissingInBucketB,
                        tail
                            ? $"Object exists in Bucket A only (Tail). Size: {itemA!.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}"
                            : $"Object exists in Bucket A but is missing in Bucket B. Size: {itemA!.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}",
                        itemA, null);
                    CommitKey(cursor, relKeyA!, itemA, null, false, diff);
                    hasA = await enumA.MoveNextAsync(ct).ConfigureAwait(false);
                }
                else
                {
                    bool tail = itemA == null;
                    var diff = MakeDifference(
                        relKeyB!,
                        DifferenceType.MissingInBucketA,
                        tail
                            ? $"Object exists in Bucket B only (Tail). Size: {itemB!.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}"
                            : $"Object exists in Bucket B but is missing in Bucket A. Size: {itemB!.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}",
                        null, itemB);
                    CommitKey(cursor, relKeyB!, null, itemB, false, diff);
                    hasB = await enumB.MoveNextAsync(ct).ConfigureAwait(false);
                }

                await PeriodicMaintenanceStepAsync(ct).ConfigureAwait(false);
            }

            lock (_stateLock)
            {
                range.Completed = true;
                _stats.PartitionsCompleted++;
            }
        }
        finally
        {
            lock (_stateLock) { _stats.PartitionsActive--; range.Active = false; }
            if (ownsStreams)
            {
                lock (_stateLock)
                {
                    _aggregateA.Add(enumA);
                    _aggregateB.Add(enumB);
                }
                try { await enumA.DisposeAsync().ConfigureAwait(false); } catch { }
                try { await enumB.DisposeAsync().ConfigureAwait(false); } catch { }
            }
        }
    }

    private async ValueTask ThrottleObjectsAsync(RangeCursor cursor, CancellationToken ct)
    {
        if (_objectLimiter == null) return;
        if (++cursor.Tokens.Pending < _objectTokenBatch) return;
        await PayObjectTokensAsync(cursor.Tokens, ct).ConfigureAwait(false);
    }

    /// <summary>Pays the keys a worker still owes the object-rate limiter (a full batch, or the remainder when it finishes).</summary>
    private async ValueTask PayObjectTokensAsync(ObjectTokenAccount account, CancellationToken ct)
    {
        if (_objectLimiter == null || account.Pending <= 0) return;
        int tokens = account.Pending;
        account.Pending = 0;
        await _objectLimiter.WaitAsync(tokens, ct).ConfigureAwait(false);
    }

    /// <summary>Brings the database in line with how this engine run starts (resume vs. fresh).</summary>
    private async Task PrepareRunStateAsync(CancellationToken ct)
    {
        if (_resumeCheckpoint != null)
        {
            // Drops differences flushed after each range's checkpointed key (they will be recreated) and, for a legacy
            // "new run id" resume, adopts the old run's differences and marks the old run superseded.
            int removed = await DbAsync("PrepareResumeAsync", () => _scanRunRepo.PrepareResumeAsync(_scanRun.Id, _resumeCheckpoint.ScanRunId, _resumeRanges!, ct), ct).ConfigureAwait(false);
            if (removed > 0)
            {
                await LogAsync("INFO", $"Resume: removed {removed} difference(s) recorded after the checkpoint (frontier key '{FrontierKey()}', {_resumeRanges!.Count} range(s)) to avoid duplicates.").ConfigureAwait(false);
            }
        }
        else if (_scanRun.ResumeCount > 0 || _scanRun.IsResumedFromCheckpoint || _checkpointDiscarded)
        {
            // Re-opened run without a usable checkpoint: restart from scratch under the same id.
            await DbAsync("ResetRunDataAsync", () => _scanRunRepo.ResetRunDataAsync(_scanRun.Id, ct), ct).ConfigureAwait(false);
            _scanRun.IsResumedFromCheckpoint = false;
            _scanRun.ResumedFromKey = null;
            _scanRun.LastEvaluatedKey = string.Empty;
            _scanRun.DifferencesTruncated = false;
            _recordedRows = 0;
            ApplyGraceWindow(); // rebuild the notes without a stale truncation sentence
            SyncStatsToRun(_scanRun, CloneStats());
        }
    }

    private async Task ObserveStatusPersistAsync()
    {
        Task t;
        lock (_pauseLock) { t = _statusPersistTask; }
        try { await t.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false); } catch { }
    }

    /// <summary>
    /// Writes the terminal state for a cancelled/interrupted/failed run. Never throws: every step is guarded so
    /// CompleteAsync always gets its chance, and the checkpoint is only written if all differences were flushed.
    /// Runs after every worker has stopped.
    /// </summary>
    private async Task FinalizeRunAsync(RunStatus status, string message)
    {
        try { UpdateLiveStats(includeSystemDiagnostics: true); } catch { }

        _scanRun.Status = status;
        _scanRun.EndTimeUtc = DateTime.UtcNow;
        _scanRun.ErrorMessage = message;
        _scanRun.LastEvaluatedKey = FrontierKey();
        SyncStatsToRun(_scanRun, CloneStats());

        bool flushed = false;
        try
        {
            await FlushDifferencesAsync(CancellationToken.None).ConfigureAwait(false);
            flushed = true;
        }
        catch (Exception ex)
        {
            int lost;
            lock (_stateLock) { lost = _differenceBuffer.Count; }
            _scanRun.ErrorMessage = $"{message} WARNING: {lost} difference(s) could not be persisted ({ex.Message}); the checkpoint was not advanced.";
            await LogAsync("ERROR", $"Final difference flush failed for run '{_scanRun.Id}': {ex.Message}", ex.StackTrace).ConfigureAwait(false);
        }

        if (flushed)
        {
            await SaveCheckpointAsync(CancellationToken.None, throwOnRepeatedFailure: false).ConfigureAwait(false);
        }

        await ObserveStatusPersistAsync().ConfigureAwait(false);

        try
        {
            await DbAsync("CompleteAsync", () => _scanRunRepo.CompleteAsync(_scanRun, CancellationToken.None), CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await LogAsync("ERROR", $"Failed to write final state of run '{_scanRun.Id}': {ex.Message}", ex.StackTrace).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Called by every worker after each key. Flushes full difference batches (waiting for the writer: back-pressure), takes a
    /// periodic checkpoint when the writer is free, and raises progress. Only one worker writes at a time.
    /// </summary>
    private async Task PeriodicMaintenanceStepAsync(CancellationToken cancellationToken)
    {
        long items = Interlocked.Increment(ref _itemsSinceLastCheckpoint);
        long elapsedMs = _stopwatch.ElapsedMilliseconds;

        int buffered;
        lock (_stateLock) { buffered = _differenceBuffer.Count; }
        if (buffered >= DifferenceBatchSize)
        {
            await _writerGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                lock (_stateLock) { buffered = _differenceBuffer.Count; }
                if (buffered >= DifferenceBatchSize) await FlushDifferencesCoreAsync(cancellationToken).ConfigureAwait(false);
            }
            finally { _writerGate.Release(); }
        }
        else if (buffered > 0 && elapsedMs - Interlocked.Read(ref _lastBufferFlushTime) > CheckpointIntervalMs && _writerGate.Wait(0))
        {
            try { await FlushDifferencesCoreAsync(cancellationToken).ConfigureAwait(false); }
            finally { _writerGate.Release(); }
        }

        if ((elapsedMs - Interlocked.Read(ref _lastCheckpointTime) > CheckpointIntervalMs || items >= _checkpointItemThreshold) && _writerGate.Wait(0))
        {
            try
            {
                Interlocked.Exchange(ref _lastCheckpointTime, elapsedMs);
                Interlocked.Exchange(ref _itemsSinceLastCheckpoint, 0);
                UpdateLiveStats(includeSystemDiagnostics: true);
                await SaveCheckpointCoreAsync(cancellationToken, throwOnRepeatedFailure: true).ConfigureAwait(false);
            }
            finally { _writerGate.Release(); }
        }

        long lastProgress = Interlocked.Read(ref _lastProgressReportTime);
        if (elapsedMs - lastProgress > ProgressIntervalMs && Interlocked.CompareExchange(ref _lastProgressReportTime, elapsedMs, lastProgress) == lastProgress)
        {
            UpdateLiveStats(includeSystemDiagnostics: true);
            RaiseProgress();
        }
    }

    private void RaiseProgress()
    {
        var handler = ProgressUpdated;
        if (handler == null) return;
        var snapshot = CloneStats();
        try { handler(snapshot); }
        catch (Exception ex) { ReportHandlerFailure("ProgressUpdated", ex); }
    }

    private void RaiseDifferenceFound(RecordedDifference diff)
    {
        var handler = DifferenceFound;
        if (handler == null) return;
        try { handler(diff); }
        catch (Exception ex) { ReportHandlerFailure("DifferenceFound", ex); }
    }

    private void ReportHandlerFailure(string eventName, Exception ex)
    {
        // Subscriber bugs must never fail a scan; log the first one only.
        if (Interlocked.Exchange(ref _handlerFailureLogged, 1) == 0)
        {
            _ = LogAsync("WARN", $"Event handler for {eventName} threw and was ignored: {ex.Message}", ex.StackTrace);
        }
    }

    private RecordedDifference MakeDifference(string key, DifferenceType type, string details, S3ObjectSummary? a, S3ObjectSummary? b)
    {
        bool isWithinGrace = false;
        if (_scanRun.GracePeriodCutoffUtc.HasValue)
        {
            var cutoff = _scanRun.GracePeriodCutoffUtc.Value;
            if ((a != null && a.LastModified >= cutoff) || (b != null && b.LastModified >= cutoff))
            {
                isWithinGrace = true;
            }
        }

        // Id is intentionally left 0: the database assigns it on insert. Key is relative to the bucket prefix.
        return new RecordedDifference
        {
            ScanRunId = _scanRun.Id,
            Key = key,
            Type = type,
            Details = details,
            BucketASize = a?.Size,
            BucketBSize = b?.Size,
            BucketAETag = a?.ETag,
            BucketBETag = b?.ETag,
            BucketALastModified = a?.LastModified,
            BucketBLastModified = b?.LastModified,
            BucketAStorageClass = a?.StorageClass,
            BucketBStorageClass = b?.StorageClass,
            DetectedAtUtc = DateTime.UtcNow,
            IsWithinGracePeriod = isWithinGrace,
            GracePeriodCutoffUtc = _scanRun.GracePeriodCutoffUtc
        };
    }

    // Caller holds _stateLock.
    private void ApplyDifferenceStats(RecordedDifference diff)
    {
        if (diff.IsWithinGracePeriod)
        {
            _stats.InFlightDifferencesCount++;
        }
        else
        {
            _stats.FilteredDifferencesCount++;
            switch (diff.Type)
            {
                case DifferenceType.MissingInBucketB: _stats.MissingInBucketBCount++; break;
                case DifferenceType.MissingInBucketA: _stats.MissingInBucketACount++; break;
                case DifferenceType.SizeMismatch: _stats.SizeMismatchCount++; break;
                case DifferenceType.ETagMismatch: _stats.ETagMismatchCount++; break;
                case DifferenceType.ContentMismatch: _stats.ContentMismatchCount++; break;
                case DifferenceType.PossibleMissingOnBothSides: _stats.SequenceGapCount++; break;
                case DifferenceType.ContentCheckError: _stats.ContentCheckErrorCount++; break;
            }
        }
        _stats.TotalDifferences++;
    }

    /// <summary>
    /// Atomically (no awaits, no cancellation points, one lock) records the outcome of one key: counters, buffered
    /// differences, consumed-object tallies and finally the range's completed-key marker.
    /// </summary>
    private void CommitKey(RangeCursor cursor, string relKey, S3ObjectSummary? a, S3ObjectSummary? b, bool matched, RecordedDifference? diff)
    {
        List<RecordedDifference>? toRecord = null;

        if (cursor.GapDetector != null)
        {
            long unusedIdCounter = 0; // detector API legacy; ids are assigned by the database
            var gaps = cursor.GapDetector.CheckKey(relKey, _scanRun.Id, ref unusedIdCounter);
            if (gaps.Count > 0) toRecord = gaps;
        }
        if (diff != null) (toRecord ??= new List<RecordedDifference>(1)).Add(diff);

        bool truncatedNow = false;
        lock (_stateLock)
        {
            _stats.ObjectsCompared++;
            if (matched) _stats.MatchedCount++;
            if (a != null) { _stats.BucketAObjectsRead++; _stats.BucketABytesRead += a.Size; _stats.CurrentKeyA = a.Key; }
            if (b != null) { _stats.BucketBObjectsRead++; _stats.BucketBBytesRead += b.Size; _stats.CurrentKeyB = b.Key; }

            if (toRecord != null)
            {
                foreach (var d in toRecord)
                {
                    // Counters always count every difference; rows stop at the cap so one bad run cannot fill the disk.
                    ApplyDifferenceStats(d);
                    if (_maxRecordedDifferences <= 0 || _recordedRows < _maxRecordedDifferences)
                    {
                        _differenceBuffer.Add(d);
                        _recordedRows++;
                    }
                    else if (!_scanRun.DifferencesTruncated)
                    {
                        _scanRun.DifferencesTruncated = true;
                        AppendTruncationNote();
                        truncatedNow = true;
                    }
                }
            }

            cursor.Range.LastCompletedKey = relKey;
            cursor.Range.ObjectsCompared++;
        }

        if (truncatedNow)
        {
            _ = LogAsync("WARN", $"Scan run '{_scanRun.Id}' reached the limit of {_maxRecordedDifferences:N0} recorded differences; further differences are counted but not stored.");
        }
        if (toRecord != null)
        {
            foreach (var d in toRecord) RaiseDifferenceFound(d);
        }
    }

    /// <summary>Persists buffered differences (taking the writer gate). The failure propagates; nothing is lost.</summary>
    private async Task FlushDifferencesAsync(CancellationToken cancellationToken)
    {
        await _writerGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { await FlushDifferencesCoreAsync(cancellationToken).ConfigureAwait(false); }
        finally { _writerGate.Release(); }
    }

    /// <summary>Swaps the buffer out and inserts it. On failure the batch goes back to the front of the buffer. Caller holds the writer gate.</summary>
    private async Task FlushDifferencesCoreAsync(CancellationToken cancellationToken)
    {
        List<RecordedDifference> batch;
        lock (_stateLock)
        {
            if (_differenceBuffer.Count == 0) return;
            batch = _differenceBuffer;
            _differenceBuffer = new List<RecordedDifference>();
        }

        await InsertOrRestoreAsync(batch, cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref _lastBufferFlushTime, _stopwatch.ElapsedMilliseconds);
    }

    private async Task InsertOrRestoreAsync(List<RecordedDifference> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0) return;
        try
        {
            await InsertBatchWithRetryAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            lock (_stateLock)
            {
                batch.AddRange(_differenceBuffer);
                _differenceBuffer = batch;
            }
            throw;
        }
    }

    private async Task InsertBatchWithRetryAsync(List<RecordedDifference> batch, CancellationToken cancellationToken)
    {
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                await DbAsync("InsertBatchAsync", () => _diffRepo.InsertBatchAsync(batch, cancellationToken), cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < 3 && !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(100 * attempt, cancellationToken).ConfigureAwait(false);
                if (attempt == 2)
                {
                    await LogAsync("WARN", $"Retrying difference batch insert ({batch.Count} items) after error: {ex.Message}").ConfigureAwait(false);
                }
            }
        }
    }

    private async Task<bool> SaveCheckpointAsync(CancellationToken cancellationToken, bool throwOnRepeatedFailure)
    {
        await _writerGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { return await SaveCheckpointCoreAsync(cancellationToken, throwOnRepeatedFailure).ConfigureAwait(false); }
        finally { _writerGate.Release(); }
    }

    /// <summary>
    /// Consistent checkpoint (caller holds the writer gate): under the state lock, swap the difference buffer and snapshot the
    /// plan and counters; insert the swapped differences; then save the snapshot. Keys committed after the snapshot are not
    /// claimed by it, and every difference of a key it claims is stored before it is written.
    /// </summary>
    private async Task<bool> SaveCheckpointCoreAsync(CancellationToken cancellationToken, bool throwOnRepeatedFailure)
    {
        List<RecordedDifference> batch;
        List<PartitionRange> plan;
        ComparisonStatistics snap;
        lock (_stateLock)
        {
            if (!_ranges.Any(r => r.HasProgress)) return true; // nothing processed yet - nothing to resume from
            batch = _differenceBuffer;
            _differenceBuffer = new List<RecordedDifference>();
            plan = _ranges.Select(r => r.Clone()).ToList();
            snap = CloneStatsUnlocked();
        }

        // Propagates (with the batch restored) if the differences cannot be stored: the checkpoint must not advance then.
        await InsertOrRestoreAsync(batch, cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref _lastBufferFlushTime, _stopwatch.ElapsedMilliseconds);

        var (hasKey, frontier) = PartitionPlanner.Frontier(plan);
        try
        {
            var cp = new ScanCheckpoint
            {
                Version = 2,
                ScanRunId = _scanRun.Id,
                JobId = _job.Id,
                CreatedAtUtc = _scanRun.StartTimeUtc,
                LastUpdatedAtUtc = DateTime.UtcNow,
                BucketNameA = _job.BucketA.BucketName,
                EndpointA = _job.BucketA.EndpointUrl,
                PrefixA = _job.BucketA.Prefix ?? string.Empty,
                BucketNameB = _job.BucketB.BucketName,
                EndpointB = _job.BucketB.EndpointUrl,
                PrefixB = _job.BucketB.Prefix ?? string.Empty,
                Mode = _job.Mode,
                LastEvaluatedKey = frontier,
                HasLastEvaluatedKey = hasKey,
                Partitions = plan,
                ObjectsCompared = snap.ObjectsCompared,
                MatchedCount = snap.MatchedCount,
                TotalDifferences = snap.TotalDifferences,
                FilteredDifferencesCount = snap.FilteredDifferencesCount,
                InFlightDifferencesCount = snap.InFlightDifferencesCount,
                MissingInBucketBCount = snap.MissingInBucketBCount,
                MissingInBucketACount = snap.MissingInBucketACount,
                SizeMismatchCount = snap.SizeMismatchCount,
                ETagMismatchCount = snap.ETagMismatchCount,
                ContentMismatchCount = snap.ContentMismatchCount,
                SequenceGapCount = snap.SequenceGapCount,
                ContentCheckErrorCount = snap.ContentCheckErrorCount,
                BucketAObjectsRead = snap.BucketAObjectsRead,
                BucketBObjectsRead = snap.BucketBObjectsRead,
                BucketABytesRead = snap.BucketABytesRead,
                BucketBBytesRead = snap.BucketBBytesRead,
                ElapsedSeconds = ElapsedSeconds(),
                DetectSequenceGaps = _job.DetectSequenceGaps,
                MaxSequenceGapThreshold = _job.MaxSequenceGapThreshold
            };

            await DbAsync("SaveCheckpointAsync", () => _scanRunRepo.SaveCheckpointAsync(cp, cancellationToken), cancellationToken).ConfigureAwait(false);

            // The run row carries the same snapshot, so its counters always match the differences stored for it.
            if (_scanRun.Status is RunStatus.Running or RunStatus.Paused)
            {
                _scanRun.LastEvaluatedKey = frontier;
                snap.ElapsedTime = TimeSpan.FromSeconds(cp.ElapsedSeconds);
                SyncStatsToRun(_scanRun, snap);
                await DbAsync("UpdateProgressAsync", () => _scanRunRepo.UpdateProgressAsync(_scanRun, cancellationToken), cancellationToken).ConfigureAwait(false);
            }

            lock (_stateLock)
            {
                _stats.LastCheckpointKey = frontier;
                _stats.LastCheckpointTime = DateTime.UtcNow;
            }
            _consecutiveCheckpointFailures = 0;
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _consecutiveCheckpointFailures++;
            await LogAsync("WARN", $"Checkpoint write failed ({_consecutiveCheckpointFailures} consecutive): {ex.Message}", ex.StackTrace).ConfigureAwait(false);

            if (throwOnRepeatedFailure && _consecutiveCheckpointFailures >= MaxConsecutiveCheckpointFailures)
            {
                throw new InvalidOperationException(
                    $"Checkpoint persistence failed {_consecutiveCheckpointFailures} consecutive times; aborting the scan because it could not be resumed safely. Last error: {ex.Message}", ex);
            }
            return false;
        }
    }

    /// <summary>Storage classes and modification-time range of everything one bucket side listed (all partitions).</summary>
    private sealed class BucketAggregate
    {
        public readonly Dictionary<string, long> StorageClassCounts = new(StringComparer.OrdinalIgnoreCase);
        public DateTime? Oldest;
        public DateTime? Newest;

        public void Add(IS3ObjectStream s)
        {
            foreach (var kv in s.StorageClassCounts)
            {
                StorageClassCounts.TryGetValue(kv.Key, out long n);
                StorageClassCounts[kv.Key] = n + kv.Value;
            }
            if (s.OldestLastModifiedUtc is DateTime o && (Oldest == null || o < Oldest)) Oldest = o;
            if (s.NewestLastModifiedUtc is DateTime n2 && (Newest == null || n2 > Newest)) Newest = n2;
        }
    }

    private async Task RecordBucketMetadataSnapshotsAsync(CancellationToken cancellationToken)
    {
        try
        {
            string? versioningA = null;
            string? versioningB = null;
            if (_clientA != null && !string.IsNullOrWhiteSpace(_job.BucketA.BucketName))
            {
                try
                {
                    var verRespA = await _clientA.GetBucketVersioningAsync(new GetBucketVersioningRequest { BucketName = _job.BucketA.BucketName }, cancellationToken).ConfigureAwait(false);
                    versioningA = verRespA.VersioningConfig?.Status?.Value ?? "Disabled";
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch { versioningA = "Unknown"; }
            }
            if (_clientB != null && !string.IsNullOrWhiteSpace(_job.BucketB.BucketName))
            {
                try
                {
                    var verRespB = await _clientB.GetBucketVersioningAsync(new GetBucketVersioningRequest { BucketName = _job.BucketB.BucketName }, cancellationToken).ConfigureAwait(false);
                    versioningB = verRespB.VersioningConfig?.Status?.Value ?? "Disabled";
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch { versioningB = "Unknown"; }
            }

            ComparisonStatistics stats;
            string classesA, classesB;
            DateTime? oldestA, newestA, oldestB, newestB;
            lock (_stateLock)
            {
                stats = CloneStatsUnlocked();
                classesA = JsonSerializer.Serialize(_aggregateA.StorageClassCounts);
                classesB = JsonSerializer.Serialize(_aggregateB.StorageClassCounts);
                (oldestA, newestA, oldestB, newestB) = (_aggregateA.Oldest, _aggregateA.Newest, _aggregateB.Oldest, _aggregateB.Newest);
            }

            var snapA = new BucketMetadataSnapshot
            {
                ScanRunId = _scanRun.Id,
                BucketSide = "A",
                BucketName = _job.BucketA.BucketName,
                EndpointUrl = _job.BucketA.EndpointUrl,
                Region = _job.BucketA.Region,
                Prefix = _job.BucketA.Prefix,
                SnapshotTimeUtc = DateTime.UtcNow,
                TotalObjects = stats.BucketAObjectsRead,
                TotalBytes = stats.BucketABytesRead,
                VersioningStatus = versioningA,
                StorageClassesJson = classesA,
                OldestLastModifiedUtc = oldestA,
                NewestLastModifiedUtc = newestA
            };

            var snapB = new BucketMetadataSnapshot
            {
                ScanRunId = _scanRun.Id,
                BucketSide = "B",
                BucketName = _job.BucketB.BucketName,
                EndpointUrl = _job.BucketB.EndpointUrl,
                Region = _job.BucketB.Region,
                Prefix = _job.BucketB.Prefix,
                SnapshotTimeUtc = DateTime.UtcNow,
                TotalObjects = stats.BucketBObjectsRead,
                TotalBytes = stats.BucketBBytesRead,
                VersioningStatus = versioningB,
                StorageClassesJson = classesB,
                OldestLastModifiedUtc = oldestB,
                NewestLastModifiedUtc = newestB
            };

            await DbAsync("SaveMetadataSnapshotAsync", () => _scanRunRepo.SaveMetadataSnapshotAsync(snapA, cancellationToken), cancellationToken).ConfigureAwait(false);
            await DbAsync("SaveMetadataSnapshotAsync", () => _scanRunRepo.SaveMetadataSnapshotAsync(snapB, cancellationToken), cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Non-critical for the scan result, but never silent.
            await LogAsync("WARN", $"Failed to record bucket metadata snapshots for run '{_scanRun.Id}': {ex.Message}", ex.StackTrace).ConfigureAwait(false);
        }
    }

    private async Task<(bool Matched, RecordedDifference? Diff)> CompareSameKeyAsync(
        string relativeKey,
        S3ObjectSummary itemA,
        S3ObjectSummary itemB,
        CancellationToken cancellationToken)
    {
        // 1. Size Check
        if (itemA.Size != itemB.Size)
        {
            return (false, MakeDifference(
                relativeKey,
                DifferenceType.SizeMismatch,
                $"Size mismatch: A has {itemA.FormattedSize} ({itemA.Size} B), B has {itemB.FormattedSize} ({itemB.Size} B)",
                itemA, itemB));
        }

        // 2. ETag Check
        if (string.Equals(itemA.ETag, itemB.ETag, StringComparison.OrdinalIgnoreCase))
        {
            return (true, null);
        }

        // 3. ETags Differ
        if (_job.Mode == ComparisonMode.ListingOnly || _readerA == null || _readerB == null)
        {
            return (false, MakeDifference(
                relativeKey,
                DifferenceType.ETagMismatch,
                $"ETags differ (A: {itemA.ETag}, B: {itemB.ETag}). Size is identical ({itemA.FormattedSize}). Note: multipart uploads have different ETags even if content matches.",
                itemA, itemB));
        }

        // Full Data stream compare using each bucket's full key; bounded across all partitions.
        await _contentChecks.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await ContentCheckAsync(relativeKey, itemA, itemB, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _contentChecks.Release();
        }
    }

    /// <summary>
    /// Compares the content of one key. A read failure affects only this key: it becomes a <see cref="DifferenceType.ContentCheckError"/>
    /// (after one retry for transient errors), or an in-flight one when the object disappeared during the scan. Only the scan's
    /// own cancellation propagates.
    /// </summary>
    private async Task<(bool Matched, RecordedDifference? Diff)> ContentCheckAsync(string relativeKey, S3ObjectSummary itemA, S3ObjectSummary itemB, CancellationToken cancellationToken)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                bool contentMatches = await StreamCompareObjectsAsync(itemA.Key, itemB.Key, cancellationToken).ConfigureAwait(false);
                if (contentMatches) return (true, null);
                return (false, MakeDifference(
                    relativeKey,
                    DifferenceType.ContentMismatch,
                    $"Content byte mismatch! Size is identical ({itemA.FormattedSize}) but data stream differs.",
                    itemA, itemB));
            }
            catch (ContentReadException ex) when (!cancellationToken.IsCancellationRequested)
            {
                if (IsNoSuchKey(ex.InnerException))
                {
                    // Listed, then gone before it could be read: the bucket changed during the scan. Recorded as in-flight
                    // (like a change inside the replication grace window), not as a confirmed disparity.
                    var gone = MakeDifference(
                        relativeKey,
                        DifferenceType.ContentCheckError,
                        $"Object was deleted or replaced in bucket {ex.Side} during the scan (NoSuchKey); content not compared.",
                        itemA, itemB);
                    gone.IsWithinGracePeriod = true;
                    return (false, gone);
                }
                if (attempt == 1 && IsTransient(ex.InnerException))
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
                    continue;
                }
                return (false, MakeDifference(
                    relativeKey,
                    DifferenceType.ContentCheckError,
                    $"Content of bucket {ex.Side} could not be read{(attempt > 1 ? " (also after one retry)" : string.Empty)}: {Describe(ex.InnerException)}. The object was not verified.",
                    itemA, itemB));
            }
        }
    }

    /// <summary>A failure to open or read one side's object content (wraps the original exception).</summary>
    private sealed class ContentReadException : Exception
    {
        public ContentReadException(string side, Exception inner) : base($"Bucket {side}: {inner.Message}", inner) { Side = side; }
        public string Side { get; }
        public new Exception InnerException => base.InnerException!;
    }

    private static bool IsNoSuchKey(Exception ex) =>
        ex is AmazonS3Exception s3 && (string.Equals(s3.ErrorCode, "NoSuchKey", StringComparison.Ordinal) || s3.StatusCode == System.Net.HttpStatusCode.NotFound);

    /// <summary>Errors worth one retry: throttling, 5xx, and transport failures (network, timeouts). Not 403 or InvalidObjectState.</summary>
    private static bool IsTransient(Exception ex) => ex switch
    {
        Amazon.Runtime.AmazonServiceException s => (int)s.StatusCode >= 500 || (int)s.StatusCode == 429
            || string.Equals(s.ErrorCode, "SlowDown", StringComparison.Ordinal) || string.Equals(s.ErrorCode, "RequestTimeout", StringComparison.Ordinal)
            || s.StatusCode == 0,
        Amazon.Runtime.AmazonClientException => true,
        IOException or HttpRequestException or TimeoutException or OperationCanceledException => true,
        _ => false
    };

    private static string Describe(Exception ex) => ex switch
    {
        Amazon.Runtime.AmazonServiceException s when !string.IsNullOrEmpty(s.ErrorCode) => $"{s.ErrorCode} ({(int)s.StatusCode}) {s.Message}",
        OperationCanceledException => "request timed out",
        _ => $"{ex.GetType().Name}: {ex.Message}"
    };

    /// <summary>Runs one side's S3 call; any failure except the scan's own cancellation is wrapped with the side.</summary>
    private static async Task<T> OnSideAsync<T>(string side, Func<Task<T>> call, CancellationToken cancellationToken)
    {
        try
        {
            return await call().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not ContentReadException && !(ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
        {
            throw new ContentReadException(side, ex);
        }
    }

    private async Task<bool> StreamCompareObjectsAsync(
        string keyA,
        string keyB,
        CancellationToken cancellationToken)
    {
        if (_limiterA != null) await _limiterA.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (_limiterB != null) await _limiterB.WaitAsync(cancellationToken).ConfigureAwait(false);

        await using var streamA = await OnSideAsync("A", () => _readerA!.OpenReadAsync(_job.BucketA.BucketName, keyA, cancellationToken), cancellationToken).ConfigureAwait(false);
        await using var streamB = await OnSideAsync("B", () => _readerB!.OpenReadAsync(_job.BucketB.BucketName, keyB, cancellationToken), cancellationToken).ConfigureAwait(false);

        byte[] bufA = new byte[65536];
        byte[] bufB = new byte[65536];

        while (true)
        {
            int bytesReadA = await OnSideAsync("A", () => ReadFullBufferAsync(streamA, bufA, cancellationToken), cancellationToken).ConfigureAwait(false);
            int bytesReadB = await OnSideAsync("B", () => ReadFullBufferAsync(streamB, bufB, cancellationToken), cancellationToken).ConfigureAwait(false);

            if (bytesReadA != bytesReadB)
                return false;

            if (bytesReadA == 0)
                return true;

            if (!bufA.AsSpan(0, bytesReadA).SequenceEqual(bufB.AsSpan(0, bytesReadB)))
                return false;
        }
    }

    private static async Task<int> ReadFullBufferAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken).ConfigureAwait(false);
            if (read == 0)
                break;
            totalRead += read;
        }
        return totalRead;
    }

    private async Task CheckPauseAsync(CancellationToken cancellationToken)
    {
        Task? waitTask = null;
        lock (_pauseLock)
        {
            if (_isPaused && _pauseTcs != null)
            {
                waitTask = _pauseTcs.Task;
            }
        }

        if (waitTask != null)
        {
            await waitTask.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private double ElapsedSeconds() => _stopwatch.Elapsed.TotalSeconds + _baselineElapsedSeconds;

    private void UpdateLiveStats(bool includeSystemDiagnostics = false)
    {
        double totalSeconds = ElapsedSeconds();
        double workingSet = includeSystemDiagnostics ? Environment.WorkingSet / (1024.0 * 1024.0) : 0;
        double gcTotal = includeSystemDiagnostics ? GC.GetTotalMemory(false) / (1024.0 * 1024.0) : 0;
        lock (_stateLock)
        {
            _stats.ElapsedTime = TimeSpan.FromSeconds(totalSeconds);
            _stats.ItemsPerSecond = totalSeconds > 0 ? _stats.ObjectsCompared / totalSeconds : 0;
            if (includeSystemDiagnostics)
            {
                _stats.WorkingSetMB = workingSet;
                _stats.GCTotalMemoryMB = gcTotal;
            }
        }
    }

    private static void SyncStatsToRun(ScanRun run, ComparisonStatistics s)
    {
        run.ObjectsCompared = s.ObjectsCompared;
        run.MatchedCount = s.MatchedCount;
        run.TotalDifferences = s.TotalDifferences;
        run.FilteredDifferencesCount = s.FilteredDifferencesCount;
        run.InFlightDifferencesCount = s.InFlightDifferencesCount;
        run.MissingInBucketBCount = s.MissingInBucketBCount;
        run.MissingInBucketACount = s.MissingInBucketACount;
        run.SizeMismatchCount = s.SizeMismatchCount;
        run.ETagMismatchCount = s.ETagMismatchCount;
        run.ContentMismatchCount = s.ContentMismatchCount;
        run.SequenceGapCount = s.SequenceGapCount;
        run.ContentCheckErrorCount = s.ContentCheckErrorCount;
        run.BucketAObjectsRead = s.BucketAObjectsRead;
        run.BucketBObjectsRead = s.BucketBObjectsRead;
        run.BucketABytesRead = s.BucketABytesRead;
        run.BucketBBytesRead = s.BucketBBytesRead;
        run.DurationSeconds = s.ElapsedTime.TotalSeconds;
    }

    private ComparisonStatistics CloneStats()
    {
        lock (_stateLock) return CloneStatsUnlocked();
    }

    // Caller holds _stateLock.
    private ComparisonStatistics CloneStatsUnlocked()
    {
        return new ComparisonStatistics
        {
            ObjectsCompared = _stats.ObjectsCompared,
            MatchedCount = _stats.MatchedCount,
            TotalDifferences = _stats.TotalDifferences,
            FilteredDifferencesCount = _stats.FilteredDifferencesCount,
            InFlightDifferencesCount = _stats.InFlightDifferencesCount,
            MissingInBucketBCount = _stats.MissingInBucketBCount,
            MissingInBucketACount = _stats.MissingInBucketACount,
            SizeMismatchCount = _stats.SizeMismatchCount,
            ETagMismatchCount = _stats.ETagMismatchCount,
            ContentMismatchCount = _stats.ContentMismatchCount,
            SequenceGapCount = _stats.SequenceGapCount,
            ContentCheckErrorCount = _stats.ContentCheckErrorCount,
            BucketAObjectsRead = _stats.BucketAObjectsRead,
            BucketBObjectsRead = _stats.BucketBObjectsRead,
            BucketABytesRead = _stats.BucketABytesRead,
            BucketBBytesRead = _stats.BucketBBytesRead,
            ElapsedTime = _stats.ElapsedTime,
            ItemsPerSecond = _stats.ItemsPerSecond,
            CurrentKeyA = _stats.CurrentKeyA,
            CurrentKeyB = _stats.CurrentKeyB,
            WorkingSetMB = _stats.WorkingSetMB,
            GCTotalMemoryMB = _stats.GCTotalMemoryMB,
            LastCheckpointKey = _stats.LastCheckpointKey,
            LastCheckpointTime = _stats.LastCheckpointTime,
            PartitionsTotal = _stats.PartitionsTotal,
            PartitionsCompleted = _stats.PartitionsCompleted,
            PartitionsActive = _stats.PartitionsActive
        };
    }
}
