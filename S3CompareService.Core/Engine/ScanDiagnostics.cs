using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace S3CompareService.Core.Engine;

/// <summary>Where a scan is: shown in the UI banner and in /api/status.</summary>
public enum ScanPhase
{
    /// <summary>Opening the database state, creating S3 clients.</summary>
    Preparing,
    /// <summary>The partition planner is looking for key-range boundaries.</summary>
    Planning,
    /// <summary>Workers are comparing key ranges.</summary>
    Scanning,
    /// <summary>All ranges are done; flushing differences, writing metadata and the final run state.</summary>
    Finalizing
}

/// <summary>An operation that has started and not yet finished (an S3 request or a database call).</summary>
public class InFlightOp
{
    private string? _description;

    internal InFlightOp(long id, string kind, string? description)
    {
        Id = id;
        Kind = kind;
        _description = description;
        StartTimestamp = Stopwatch.GetTimestamp();
    }

    public long Id { get; }
    /// <summary>"S3" or "DB".</summary>
    public string Kind { get; }
    /// <summary>What the operation is. Built on first use, so a request that finishes quickly never formats it.</summary>
    public string Description => _description ??= Describe();
    protected virtual string Describe() => string.Empty;
    internal long StartTimestamp { get; }
    internal int Warned;
    private int _attempt = 1;

    /// <summary>Failed attempts that were retried (set by <see cref="S3Lister"/>); 0 when the request never needed a retry.</summary>
    internal int Retries;
    /// <summary>Why the last retried attempt failed ("timed out after 30 s", ...).</summary>
    internal string? RetryReason;

    /// <summary>1 for the first try, higher while the request is being retried.</summary>
    public int Attempt { get => Volatile.Read(ref _attempt); set => Volatile.Write(ref _attempt, value); }

    public double AgeSeconds => Stopwatch.GetElapsedTime(StartTimestamp).TotalSeconds;
}

/// <summary>A copy of an in-flight operation for the API.</summary>
public sealed class InFlightInfo
{
    public string Kind { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public double AgeSeconds { get; set; }
    public int Attempt { get; set; } = 1;

    public override string ToString() => $"{Kind}: {Description} (in flight {ScanDiagnosticsFormatter.Seconds(AgeSeconds)}{(Attempt > 1 ? $", attempt {Attempt}" : string.Empty)})";
}

/// <summary>Registry of the operations in flight right now. Thread-safe.</summary>
public sealed class InFlightTracker
{
    private readonly ConcurrentDictionary<long, InFlightOp> _ops = new();
    private long _nextId;

    /// <summary>The S3 request running on the current async flow (set by <see cref="InstrumentedLister"/>), so retry loops can report attempts.</summary>
    public static readonly AsyncLocal<InFlightOp?> CurrentRequest = new();

    public sealed class Scope : IDisposable
    {
        private readonly InFlightTracker _owner;
        public InFlightOp Op { get; }
        internal Scope(InFlightTracker owner, InFlightOp op) { _owner = owner; Op = op; }
        public void Dispose() => _owner._ops.TryRemove(Op.Id, out _);
    }

    public Scope Begin(string kind, string description)
    {
        var op = new InFlightOp(Interlocked.Increment(ref _nextId), kind, description);
        _ops[op.Id] = op;
        return new Scope(this, op);
    }

    internal Scope Begin(Func<long, InFlightOp> factory)
    {
        var op = factory(Interlocked.Increment(ref _nextId));
        _ops[op.Id] = op;
        return new Scope(this, op);
    }

    public List<InFlightOp> Current() => _ops.Values.OrderByDescending(o => o.AgeSeconds).ToList();

    public List<InFlightInfo> Snapshot(int max = 8) =>
        Current().Take(max).Select(o => new InFlightInfo { Kind = o.Kind, Description = o.Description, AgeSeconds = o.AgeSeconds, Attempt = o.Attempt }).ToList();

    public InFlightInfo? Oldest(string kind)
    {
        var o = Current().FirstOrDefault(x => x.Kind == kind);
        return o == null ? null : new InFlightInfo { Kind = o.Kind, Description = o.Description, AgeSeconds = o.AgeSeconds, Attempt = o.Attempt };
    }

    /// <summary>Operations older than <paramref name="seconds"/> that were not reported before (each is returned once).</summary>
    public List<InFlightOp> TakeNewlyOverdue(string kind, double seconds) =>
        _ops.Values.Where(o => o.Kind == kind && o.AgeSeconds >= seconds && !o.Description.StartsWith("Audit", StringComparison.Ordinal) && Interlocked.Exchange(ref o.Warned, 1) == 0).ToList();
}

/// <summary>Request statistics of one bucket side, fed by <see cref="InstrumentedLister"/> and <see cref="InstrumentedLimiter"/>.</summary>
public sealed class BucketMetrics
{
    private const double RateWindowSeconds = 10;
    private readonly object _gate = new();
    private readonly Queue<long> _recent = new();
    private readonly long _created = Stopwatch.GetTimestamp();
    private long _requests, _errors, _retried, _recovered, _completed, _latencyTicks, _lastLatencyTicks, _maxLatencyTicks, _throttleTicks;
    private int _inFlight;
    private string? _lastError;

    public BucketMetrics(string side, string bucket, double rateLimitPerSecond)
    {
        Side = side;
        Bucket = bucket;
        RateLimit = rateLimitPerSecond;
    }

    public string Side { get; }
    public string Bucket { get; }
    public double RateLimit { get; }

    internal void RequestStarted()
    {
        Interlocked.Increment(ref _requests);
        Interlocked.Increment(ref _inFlight);
        lock (_gate)
        {
            _recent.Enqueue(Stopwatch.GetTimestamp());
            Trim();
        }
    }

    internal void RequestFinished(long ticks, string? error, int retries = 0)
    {
        Interlocked.Decrement(ref _inFlight);
        if (retries > 0)
        {
            Interlocked.Increment(ref _retried);
            if (error == null) Interlocked.Increment(ref _recovered);
        }
        if (error == null)
        {
            Interlocked.Increment(ref _completed);
            Interlocked.Add(ref _latencyTicks, ticks);
            Interlocked.Exchange(ref _lastLatencyTicks, ticks);
            long seen;
            while (ticks > (seen = Interlocked.Read(ref _maxLatencyTicks)) && Interlocked.CompareExchange(ref _maxLatencyTicks, ticks, seen) != seen) { }
        }
        else
        {
            Interlocked.Increment(ref _errors);
            _lastError = error;
        }
    }

    internal void AddThrottle(long ticks) => Interlocked.Add(ref _throttleTicks, ticks);

    private void Trim()
    {
        long limit = Stopwatch.GetTimestamp() - (long)(RateWindowSeconds * Stopwatch.Frequency);
        while (_recent.Count > 0 && _recent.Peek() < limit) _recent.Dequeue();
    }

    private static double Ms(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;

    public BucketRequestStats Snapshot()
    {
        double rate;
        lock (_gate)
        {
            Trim();
            double window = Math.Min(RateWindowSeconds, Math.Max(1, Stopwatch.GetElapsedTime(_created).TotalSeconds));
            rate = _recent.Count / window;
        }
        long completed = Interlocked.Read(ref _completed);
        return new BucketRequestStats
        {
            Side = Side,
            Bucket = Bucket,
            Requests = Interlocked.Read(ref _requests),
            Errors = Interlocked.Read(ref _errors),
            Retried = Interlocked.Read(ref _retried),
            Recovered = Interlocked.Read(ref _recovered),
            InFlight = Volatile.Read(ref _inFlight),
            RequestsPerSecond = Math.Round(rate, 1),
            RateLimitPerSecond = RateLimit,
            AvgLatencyMs = completed > 0 ? Math.Round(Ms(Interlocked.Read(ref _latencyTicks)) / completed, 0) : 0,
            LastLatencyMs = Math.Round(Ms(Interlocked.Read(ref _lastLatencyTicks)), 0),
            MaxLatencyMs = Math.Round(Ms(Interlocked.Read(ref _maxLatencyTicks)), 0),
            ThrottleWaitSeconds = Math.Round(Interlocked.Read(ref _throttleTicks) / (double)Stopwatch.Frequency, 1),
            LastError = _lastError
        };
    }
}

/// <summary>Counts, times and tracks every request through an <see cref="IS3Lister"/>. Lets a request that ignores its cancellation token be abandoned.</summary>
public sealed class InstrumentedLister : IS3Lister
{
    private readonly IS3Lister _inner;
    private readonly BucketMetrics _metrics;
    private readonly InFlightTracker _tracker;
    private readonly ScanIssues? _issues;

    public InstrumentedLister(IS3Lister inner, BucketMetrics metrics, InFlightTracker tracker, ScanIssues? issues = null)
    {
        _issues = issues;
        _inner = inner;
        _metrics = metrics;
        _tracker = tracker;
    }

    public async Task<S3ListPage> ListPageAsync(string bucket, string? prefix, string? startAfter, string? continuationToken, string? delimiter, int maxKeys, CancellationToken cancellationToken)
    {
        using var scope = _tracker.Begin(id => new ListOp(id, _metrics.Side, bucket, prefix, startAfter, delimiter, maxKeys, !string.IsNullOrEmpty(continuationToken)));
        // Only the real client retries and needs to report attempts; skip the async-local write for everything else.
        bool reportsAttempts = _inner is S3Lister;
        if (reportsAttempts) InFlightTracker.CurrentRequest.Value = scope.Op;
        long start = Stopwatch.GetTimestamp();
        _metrics.RequestStarted();
        string? error = null;
        try
        {
            // WaitAsync: a request that does not observe the token (a hung socket, a fake) must not hold up a cancel.
            return await _inner.ListPageAsync(bucket, prefix, startAfter, continuationToken, delimiter, maxKeys, cancellationToken)
                .WaitUnlessDoneAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            error = null;
            throw;
        }
        catch (Exception ex)
        {
            error = $"{ex.GetType().Name}: {ex.Message}";
            throw;
        }
        finally
        {
            if (reportsAttempts) InFlightTracker.CurrentRequest.Value = null;
            int retries = scope.Op.Retries;
            _metrics.RequestFinished(Stopwatch.GetTimestamp() - start, error, retries);
            // A failed attempt that a retry then fixed: the request is fine, so it is a Recovered issue, not an error.
            // (A request that failed for good is classified by its caller: skipped by the planner, fatal in a scan.)
            if (retries > 0 && error == null && _issues != null)
                _issues.Record(IssueOutcome.Recovered, $"{scope.Op.Description}: {scope.Op.RetryReason ?? "failed"} — no impact", scope.Op.Attempt);
        }
    }

    internal static string Clip(string? s) => s == null ? string.Empty : s.Length <= 60 ? s : s.Substring(0, 57) + "...";
}

/// <summary>An S3 list request in flight; its text is built only when somebody looks at it.</summary>
internal sealed class ListOp : InFlightOp
{
    private readonly string _side, _bucket;
    private readonly string? _prefix, _startAfter, _delimiter;
    private readonly int _maxKeys;
    private readonly bool _continued;

    public ListOp(long id, string side, string bucket, string? prefix, string? startAfter, string? delimiter, int maxKeys, bool continued) : base(id, "S3", null)
    {
        _side = side; _bucket = bucket; _prefix = prefix; _startAfter = startAfter; _delimiter = delimiter; _maxKeys = maxKeys; _continued = continued;
    }

    protected override string Describe() =>
        $"ListObjectsV2 bucket {_side} ({_bucket}) prefix='{InstrumentedLister.Clip(_prefix)}' startAfter='{InstrumentedLister.Clip(_startAfter)}' delimiter='{_delimiter}' maxKeys={_maxKeys}{(_continued ? " (continued)" : string.Empty)}";
}

/// <summary>Records how long callers wait in the rate limiter.</summary>
public sealed class InstrumentedLimiter : IRequestRateLimiter
{
    private readonly IRequestRateLimiter _inner;
    private readonly BucketMetrics _metrics;

    public InstrumentedLimiter(IRequestRateLimiter inner, BucketMetrics metrics)
    {
        _inner = inner;
        _metrics = metrics;
    }

    public async Task WaitAsync(CancellationToken cancellationToken)
    {
        long start = Stopwatch.GetTimestamp();
        try { await _inner.WaitAsync(cancellationToken).ConfigureAwait(false); }
        finally { _metrics.AddThrottle(Stopwatch.GetTimestamp() - start); }
    }
}

// ---------------- snapshots (JSON payloads) ----------------

/// <summary>Live numbers of the partition planner.</summary>
public sealed class PlannerStats
{
    public bool Started { get; set; }
    public bool Finished { get; set; }
    public int RequestsUsed { get; set; }
    public int RequestBudget { get; set; }
    public int Candidates { get; set; }
    public int TargetPartitions { get; set; }
    public string Step { get; set; } = string.Empty;
    public double ElapsedSeconds { get; set; }
    /// <summary>0 = no limit.</summary>
    public double TimeLimitSeconds { get; set; }
    public bool TimedOut { get; set; }
    public bool JumpDisabled { get; set; }
    /// <summary>"U+10FFFF", "~ fallback" or "disabled".</summary>
    public string JumpMode { get; set; } = "U+10FFFF";
    public int RequestErrors { get; set; }
    public string? LastError { get; set; }
    public string Method { get; set; } = "none";
    public int Ranges { get; set; }
}

public sealed class BucketRequestStats
{
    public string Side { get; set; } = string.Empty;
    public string Bucket { get; set; } = string.Empty;
    public long Requests { get; set; }
    /// <summary>Requests that failed for good (after all attempts).</summary>
    public long Errors { get; set; }
    /// <summary>Requests that needed at least one retry.</summary>
    public long Retried { get; set; }
    /// <summary>Of those, the ones that then succeeded.</summary>
    public long Recovered { get; set; }
    public int InFlight { get; set; }
    public double RequestsPerSecond { get; set; }
    public double RateLimitPerSecond { get; set; }
    public double AvgLatencyMs { get; set; }
    public double LastLatencyMs { get; set; }
    public double MaxLatencyMs { get; set; }
    public double ThrottleWaitSeconds { get; set; }
    public string? LastError { get; set; }
}

public sealed class PartitionRow
{
    public int Index { get; set; }
    public string Lower { get; set; } = string.Empty;
    public string Upper { get; set; } = string.Empty;
    /// <summary>queued, active or done.</summary>
    public string State { get; set; } = "queued";
    public string LastCompletedKey { get; set; } = string.Empty;
    public long ObjectsCompared { get; set; }
}

/// <summary>Everything the diagnostics panel and the banner show about one active scan.</summary>
public sealed class ScanDiagnosticsSnapshot
{
    public string RunId { get; set; } = string.Empty;
    public string JobName { get; set; } = string.Empty;
    /// <summary>Preparing, Planning, Scanning, Finalizing, or Paused / Cancelling while those apply.</summary>
    public string Phase { get; set; } = "Preparing";
    /// <summary>The phase itself, even while paused or cancelling.</summary>
    public string UnderlyingPhase { get; set; } = "Preparing";
    public bool IsPaused { get; set; }
    /// <summary>Seconds since a cancel was requested, or null.</summary>
    public double? CancelRequestedSeconds { get; set; }
    /// <summary>What the engine is doing right now ("Creating S3 clients", "Workers running (8)", ...).</summary>
    public string Operation { get; set; } = string.Empty;
    public double ElapsedSeconds { get; set; }
    public long ObjectsCompared { get; set; }
    public PlannerStats Planner { get; set; } = new();
    public BucketRequestStats BucketA { get; set; } = new();
    public BucketRequestStats BucketB { get; set; } = new();
    public int PartitionsTotal { get; set; }
    public int PartitionsCompleted { get; set; }
    public int PartitionsActive { get; set; }
    public int PartitionsQueued { get; set; }
    public int PartitionsOmitted { get; set; }
    /// <summary>Running ranges split so an idle worker could take their second half (work stealing).</summary>
    public int PartitionSplits { get; set; }
    public List<PartitionRow> Partitions { get; set; } = new();
    /// <summary>The text of the last FAILED issue (the scan or a range stopped), or null. Harmless issues never appear here.</summary>
    public string? LastError { get; set; }
    /// <summary>Counts per outcome, for the "Issues: 0 failed · 2 recovered · ..." line.</summary>
    public IssueSummary Issues { get; set; } = new();
    /// <summary>The most severe recent issue (Failed over Degraded over Recorded over Skipped over Recovered; most recent among equals), or null.</summary>
    public ScanIssueInfo? TopIssue { get; set; }
    /// <summary>The last issues, newest first (at most 10).</summary>
    public List<ScanIssueInfo> RecentIssues { get; set; } = new();
    public List<InFlightInfo> InFlight { get; set; } = new();
    public InFlightInfo? OldestS3 { get; set; }
    public InFlightInfo? OldestDb { get; set; }
    /// <summary>The banner text, for example "PLANNING: 132/500 requests · 41 candidates · probe round 2 · 0:48".</summary>
    public string PhaseText { get; set; } = string.Empty;
}

public static class ScanDiagnosticsFormatter
{
    /// <summary>The most partition rows a snapshot carries.</summary>
    public const int MaxPartitionRows = 256;

    public static string Clock(double seconds)
    {
        var t = TimeSpan.FromSeconds(Math.Max(0, seconds));
        return t.TotalHours >= 1 ? $"{(int)t.TotalHours}:{t.Minutes:00}:{t.Seconds:00}" : $"{t.Minutes}:{t.Seconds:00}";
    }

    public static string Seconds(double seconds) => seconds < 10 ? $"{seconds:0.0} s" : $"{seconds:0} s";

    public static string PhaseText(ScanDiagnosticsSnapshot s)
    {
        var p = s.Planner;
        string core = s.UnderlyingPhase switch
        {
            "Planning" => $"PLANNING: {p.RequestsUsed}/{p.RequestBudget} requests · {p.Candidates} candidates · {(string.IsNullOrEmpty(p.Step) ? "starting" : p.Step)} · {Clock(p.ElapsedSeconds)}"
                + (p.TimeLimitSeconds > 0 ? $" of {Clock(p.TimeLimitSeconds)}" : string.Empty)
                + (p.RequestErrors > 0 ? $" · {p.RequestErrors} request(s) skipped" : string.Empty),
            "Scanning" => $"SCANNING: {s.PartitionsCompleted}/{s.PartitionsTotal} partitions done · {s.PartitionsActive} active · {s.PartitionsQueued} queued{(s.PartitionSplits > 0 ? $" · {s.PartitionSplits} split" : string.Empty)}",
            "Finalizing" => "FINALIZING: writing the final results",
            _ => "PREPARING: " + (string.IsNullOrEmpty(s.Operation) ? "starting" : s.Operation)
        };
        if (s.CancelRequestedSeconds is double c)
        {
            string stuck = s.OldestS3 != null || s.OldestDb != null
                ? " · waiting for " + string.Join(", ", new[] { s.OldestS3, s.OldestDb }.Where(x => x != null).Select(x => $"{x!.Kind} {Seconds(x.AgeSeconds)}"))
                : string.Empty;
            return $"CANCELLING… ({c:0} s){stuck} (was {core})";
        }
        return s.IsPaused ? $"PAUSED ({core})" : core;
    }

    /// <summary>Multi-line plain text for the diagnostics panel (monospace friendly, one screenshot).</summary>
    public static string Summary(ScanDiagnosticsSnapshot s, bool includeRecentIssues = true)
    {
        var sb = new StringBuilder();
        string runShort = s.RunId.Length > 8 ? s.RunId.Substring(0, 8) : s.RunId;
        sb.AppendLine($"RUN {runShort}  JOB {s.JobName}  ELAPSED {Clock(s.ElapsedSeconds)}");
        sb.AppendLine($"PHASE    {s.PhaseText}");
        sb.AppendLine($"DOING    {(string.IsNullOrEmpty(s.Operation) ? "-" : s.Operation)}");
        var p = s.Planner;
        if (p.Started)
        {
            sb.AppendLine($"PLANNER  requests {p.RequestsUsed}/{p.RequestBudget} · candidates {p.Candidates} (target {p.TargetPartitions - 1}+) · step: {(string.IsNullOrEmpty(p.Step) ? "-" : p.Step)} · {(p.Finished ? "finished" : "running")} · elapsed {Clock(p.ElapsedSeconds)}"
                + (p.TimeLimitSeconds > 0 ? $" / limit {Clock(p.TimeLimitSeconds)}" : " / no limit")
                + $" · method {p.Method} · ranges {p.Ranges}");
            sb.AppendLine($"         skipped requests {p.RequestErrors} · time limit hit: {(p.TimedOut ? "YES" : "no")} · shared-prefix jump: {p.JumpMode}");
            if (!string.IsNullOrEmpty(p.LastError)) sb.AppendLine($"         last planner issue: {p.LastError}");
        }
        else
        {
            sb.AppendLine("PLANNER  not run (single partition, planning off, or resumed with a stored plan)");
        }
        foreach (var b in new[] { s.BucketA, s.BucketB })
        {
            sb.AppendLine($"BUCKET {b.Side}  {b.Bucket}: requests {b.Requests:N0} · {b.RequestsPerSecond:0.0}/s (limit {(b.RateLimitPerSecond > 0 ? b.RateLimitPerSecond.ToString("0.#", CultureInfo.InvariantCulture) + "/s" : "none")}) · in flight {b.InFlight} · latency avg {b.AvgLatencyMs:N0} ms, last {b.LastLatencyMs:N0} ms, max {b.MaxLatencyMs:N0} ms · throttle wait {b.ThrottleWaitSeconds:0.0} s · retried {b.Retried} (recovered {b.Recovered}) · failed {b.Errors}");
        }
        sb.AppendLine($"PARTITIONS {s.PartitionsCompleted}/{s.PartitionsTotal} done · {s.PartitionsActive} active · {s.PartitionsQueued} queued · {s.PartitionSplits} split by idle workers · {s.ObjectsCompared:N0} objects compared");
        sb.AppendLine($"OLDEST IN FLIGHT  S3: {(s.OldestS3 == null ? "none" : s.OldestS3.ToString())}");
        sb.AppendLine($"                  DB: {(s.OldestDb == null ? "none" : s.OldestDb.ToString())}");
        sb.AppendLine(s.Issues.Text);
        sb.Append($"MOST SEVERE  {(s.TopIssue == null ? "none" : s.TopIssue.Display)}");
        if (includeRecentIssues && s.RecentIssues.Count > 0)
        {
            sb.AppendLine().AppendLine();
            sb.AppendLine($"RECENT ISSUES (last {s.RecentIssues.Count}, newest first)");
            for (int i = 0; i < s.RecentIssues.Count; i++)
            {
                sb.Append("  ").Append(s.RecentIssues[i].Line);
                if (i < s.RecentIssues.Count - 1) sb.AppendLine();
            }
        }
        return sb.ToString();
    }
}

/// <summary>Per-scan collector: phase, current operation, last error, in-flight tracking and bucket metrics.</summary>
public sealed class ScanDiagnostics
{
    private volatile string _operation = string.Empty;
    private int _phase;

    public InFlightTracker InFlight { get; } = new();
    public ScanIssues Issues { get; } = new();
    public BucketMetrics BucketA { get; private set; }
    public BucketMetrics BucketB { get; private set; }

    public ScanDiagnostics()
    {
        BucketA = new BucketMetrics("A", string.Empty, 0);
        BucketB = new BucketMetrics("B", string.Empty, 0);
    }

    public void ConfigureBuckets(string bucketA, string bucketB, double rateLimit)
    {
        BucketA = new BucketMetrics("A", bucketA, rateLimit);
        BucketB = new BucketMetrics("B", bucketB, rateLimit);
    }

    public ScanPhase Phase { get => (ScanPhase)Volatile.Read(ref _phase); set => Volatile.Write(ref _phase, (int)value); }
    public string Operation { get => _operation; set => _operation = value; }
}

/// <summary>Task helpers for abandoning calls that ignore cancellation.</summary>
internal static class AbandonableTask
{
    /// <summary>Waits for the task but gives up on timeout or cancellation. A task that already finished keeps its own outcome (its real error, not a cancellation).</summary>
    public static Task<T> WaitUnlessDoneAsync<T>(this Task<T> task, TimeSpan timeout, CancellationToken ct) =>
        task.IsCompleted ? task : task.WaitAsync(timeout, ct);

    public static Task WaitUnlessDoneAsync(this Task task, TimeSpan timeout, CancellationToken ct) =>
        task.IsCompleted ? task : task.WaitAsync(timeout, ct);
}
