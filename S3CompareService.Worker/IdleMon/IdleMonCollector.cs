using Microsoft.Extensions.Logging;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Worker.IdleMon;

/// <summary>
/// The collection logic behind <see cref="IdleMonReportingService"/>, without any thread of its own so tests can drive it with a
/// fake clock. <see cref="TickAsync"/> does what is due: a collection pass every metrics interval (reads the repositories, builds
/// alerts, metrics and snapshots) and a heartbeat every heartbeat interval. The heartbeat is built from this loop's own state, so
/// a collection that fails or hangs makes it degraded, and if the loop itself dies the heartbeats stop (the hub then shows the
/// source as stale). Nothing here ever throws into the caller.
/// </summary>
public sealed class IdleMonCollector
{
    /// <summary>How many recent runs per job are read (enough for the failure streak and the newest completed run).</summary>
    public const int RunsPerJob = 50;

    /// <summary>A collection pass that takes longer than this is abandoned (a hung database must not stop the heartbeat for ever).</summary>
    public static readonly TimeSpan CollectionTimeout = TimeSpan.FromSeconds(60);

    private readonly IdleMonOptions _options;
    private readonly IJobRepository _jobs;
    private readonly IScanRunRepository _runs;
    private readonly IDifferenceRepository _differences;
    private readonly IActiveScanSource _activeScans;
    private readonly IIdleMonReporter _reporter;
    private readonly IdleMonSpool? _spool;
    private readonly Func<DateTimeOffset?> _schedulerLastEvaluation;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;

    private readonly AlertStateMachine _alerts = new();
    private readonly ScanProgressTracker _progress = new();
    private readonly ResultSendTracker _results = new();
    private readonly Dictionary<string, (string RunId, IdleMonMessage Snapshot)> _topDisparities = new();
    private readonly DateTimeOffset _startedAt;
    private readonly long _startedTs;
    private long _lastMetricsTs;    // monotonic timestamps of the last start; 0 = never
    private long _lastHeartbeatTs;
    private volatile CollectionState _state = CollectionState.Initial;
    private int _lastLoggedFailure;

    // What the last collection pass left behind, swapped as one immutable object so the heartbeat reads a consistent view
    // without a lock and without waiting for the pass.
    private sealed record CollectionState(long LastOkTimestamp, string? DatabaseError, IReadOnlyList<StuckScan> StuckScans)
    {
        public static readonly CollectionState Initial = new(0, null, []);
    }

    /// <summary>Creates the collector.</summary>
    public IdleMonCollector(
        IdleMonOptions options,
        IJobRepository jobs,
        IScanRunRepository runs,
        IDifferenceRepository differences,
        IActiveScanSource activeScans,
        IIdleMonReporter reporter,
        IdleMonSpool? spool,
        Func<DateTimeOffset?> schedulerLastEvaluation,
        TimeProvider time,
        ILogger<IdleMonCollector> logger)
    {
        _options = options;
        _jobs = jobs;
        _runs = runs;
        _differences = differences;
        _activeScans = activeScans;
        _reporter = reporter;
        _spool = spool;
        _schedulerLastEvaluation = schedulerLastEvaluation;
        _time = time;
        _logger = logger;
        _startedAt = time.GetUtcNow();
        _startedTs = time.GetTimestamp();
    }

    /// <summary>Gets the alerts firing now.</summary>
    public IReadOnlyCollection<string> FiringAlerts => _alerts.FiringKeys;

    /// <summary>Does what is due now: a collection pass and/or a heartbeat (one after the other; the hosted service runs them on separate tasks).</summary>
    public async Task TickAsync(CancellationToken cancellationToken)
    {
        await CollectIfDueAsync(cancellationToken).ConfigureAwait(false);

        // The heartbeat comes after the collection, so it reports the result of this very pass.
        HeartbeatIfDue();
    }

    /// <summary>Runs a collection pass when one is due (monotonic time).</summary>
    public async Task CollectIfDueAsync(CancellationToken cancellationToken)
    {
        if (!IsDue(ref _lastMetricsTs, _options.MetricsIntervalSeconds)) return;
        await CollectAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Sends a heartbeat when one is due (monotonic time). Reads only in-memory state, never waits for I/O.</summary>
    public bool HeartbeatIfDue()
    {
        if (!IsDue(ref _lastHeartbeatTs, _options.HeartbeatIntervalSeconds)) return false;
        SendHeartbeat();
        return true;
    }

    // Due when never done, when the interval has passed on the monotonic clock, or when the elapsed time is negative (a clock
    // that went backwards must not make the next run wait for ever). Marks the run as started.
    private bool IsDue(ref long lastTimestamp, int intervalSeconds)
    {
        long now = _time.GetTimestamp();
        long last = Interlocked.Read(ref lastTimestamp);
        if (last != 0)
        {
            var elapsed = _time.GetElapsedTime(last, now);
            if (elapsed >= TimeSpan.Zero && elapsed < TimeSpan.FromSeconds(intervalSeconds)) return false;
        }

        Interlocked.Exchange(ref lastTimestamp, now == 0 ? 1 : now);
        return true;
    }

    // Time until the next run, on the monotonic clock: at least 250 ms, at most one interval (a clock fault never makes it longer).
    private TimeSpan DelayUntil(long lastTimestamp, int intervalSeconds)
    {
        var interval = TimeSpan.FromSeconds(intervalSeconds);
        if (lastTimestamp == 0) return TimeSpan.FromMilliseconds(250);
        var remaining = interval - _time.GetElapsedTime(lastTimestamp, _time.GetTimestamp());
        if (remaining > interval) remaining = interval;
        return remaining < TimeSpan.FromMilliseconds(250) ? TimeSpan.FromMilliseconds(250) : remaining;
    }

    /// <summary>Gets how long to wait until the next collection pass is due.</summary>
    public TimeSpan DelayUntilNextCollection() => DelayUntil(Interlocked.Read(ref _lastMetricsTs), _options.MetricsIntervalSeconds);

    /// <summary>Gets how long to wait until the next heartbeat is due.</summary>
    public TimeSpan DelayUntilNextHeartbeat() => DelayUntil(Interlocked.Read(ref _lastHeartbeatTs), _options.HeartbeatIntervalSeconds);

    /// <summary>Gets how long to wait until something is due.</summary>
    public TimeSpan DelayUntilNextTick()
    {
        var a = DelayUntilNextCollection();
        var b = DelayUntilNextHeartbeat();
        return a < b ? a : b;
    }

    /// <summary>One collection pass. Never throws (except for cancellation of <paramref name="cancellationToken"/>).</summary>
    public async Task CollectAsync(CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow();
        try
        {
            var active = _activeScans.GetActiveScans();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(CollectionTimeout);

            IReadOnlyList<StuckScan> stuck;
            try
            {
                var states = await ReadStatesAsync(active, timeout.Token).ConfigureAwait(false);

                _progress.Observe(active, now);
                // Active scans are matched to the states read above; a scan that started meanwhile shows up on the next pass.
                var evaluation = AlertEvaluator.Evaluate(states, now, _options, _progress);

                foreach (var message in _alerts.Apply(evaluation.Conditions, now)) _reporter.Submit(message);
                foreach (var message in MetricBuilder.Build(states, active.Count, evaluation.OverdueJobs, now, _options, _results)) _reporter.Submit(message);
                _reporter.Submit(SnapshotBuilder.Jobs(states, now, _options));
                if (_options.IncludeObjectKeys)
                {
                    // The same timeout covers the top-disparities read: a locked database must not hold this pass for ever.
                    foreach (var message in await TopDisparitiesAsync(states, now, timeout.Token).ConfigureAwait(false)) _reporter.Submit(message);
                }

                stuck = evaluation.StuckScans;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException("the read timed out");
            }

            // One swap: the heartbeat sees either the old state or the complete new one.
            _state = new CollectionState(NonZero(_time.GetTimestamp()), null, stuck);
            _lastLoggedFailure = 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Unknown is not zero: nothing is sent for this pass, alerts keep their state, and the heartbeat says degraded.
            var previous = _state;
            _state = previous with { DatabaseError = ex is TimeoutException ? "timed out" : ex.GetType().Name };
            if (Interlocked.Exchange(ref _lastLoggedFailure, 1) == 0)
                _logger.LogWarning("IdleMon collection failed ({Type}); the heartbeat will say degraded until it works again.", ex.GetType().Name);
        }
    }

    private static long NonZero(long timestamp) => timestamp == 0 ? 1 : timestamp;

    private async Task<List<JobState>> ReadStatesAsync(IReadOnlyList<ActiveScanInfo> active, CancellationToken cancellationToken)
    {
        var jobs = await _jobs.GetAllAsync(cancellationToken).ConfigureAwait(false);
        var scansByJob = active.GroupBy(s => s.JobId).ToDictionary(g => g.Key, g => g.First());
        var states = new List<JobState>(jobs.Count);
        foreach (var job in jobs)
        {
            var runs = await _runs.GetRecentRunSummariesAsync(RunsPerJob, job.Id, cancellationToken).ConfigureAwait(false);
            scansByJob.TryGetValue(job.Id, out var scan);
            states.Add(new JobState(job, runs, scan));
        }

        return states;
    }

    private async Task<List<IdleMonMessage>> TopDisparitiesAsync(IReadOnlyList<JobState> states, DateTimeOffset now, CancellationToken cancellationToken)
    {
        var list = new List<IdleMonMessage>();
        foreach (var state in states.Where(s => s.Job.IsEnabled && s.NewestCompleted is not null))
        {
            var run = state.NewestCompleted!;
            if (now.UtcDateTime - JobState.EndOf(run) > MetricBuilder.ResultWindow(state.Job, now.UtcDateTime, _options)) continue;

            if (!_topDisparities.TryGetValue(state.Job.Id, out var cached) || cached.RunId != run.Id)
            {
                var rows = await _differences.GetDifferencesAsync(run.Id, null, false, null, 0, SnapshotBuilder.MaxDisparityRows, cancellationToken).ConfigureAwait(false);
                cached = (run.Id, SnapshotBuilder.TopDisparities(state, rows, _options));
                _topDisparities[state.Job.Id] = cached;
            }

            // A fresh message (new id) every pass; the timestamp stays the run's end time (the time of the data).
            list.Add(Clone(cached.Snapshot));
        }

        return list;
    }

    private static IdleMonMessage Clone(IdleMonMessage m) => new()
    {
        Kind = m.Kind, Timestamp = m.Timestamp, Name = m.Name, Columns = m.Columns, Rows = m.Rows, Tags = m.Tags,
    };

    /// <summary>
    /// Sends one heartbeat from the state of the last collection. It reads only memory (the last pass's result, the scheduler's
    /// last evaluation, the spool's cached backlog), so a blocked database or disk cannot delay it. Ages are measured on the
    /// monotonic clock and turned into times relative to now, so a wall-clock step cannot fake or hide a stall.
    /// </summary>
    public void SendHeartbeat()
    {
        try
        {
            var now = _time.GetUtcNow();
            var nowTs = _time.GetTimestamp();
            var state = _state;
            _reporter.Submit(HeartbeatBuilder.Build(new HeartbeatInput
            {
                Now = now,
                StartedAt = now - Positive(_time.GetElapsedTime(_startedTs, nowTs)),
                LastCollectionOk = state.LastOkTimestamp == 0 ? null : now - Positive(_time.GetElapsedTime(state.LastOkTimestamp, nowTs)),
                DatabaseError = state.DatabaseError,
                SchedulerLastEvaluation = _schedulerLastEvaluation(),
                StuckScans = state.StuckScans,
                Backlog = Backlog(now),
            }, _options));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _logger.LogWarning("IdleMon heartbeat failed ({Type}).", ex.GetType().Name);
        }
    }

    private static TimeSpan Positive(TimeSpan value) => value < TimeSpan.Zero ? TimeSpan.Zero : value;

    /// <summary>Sends the heartbeat for a clean shutdown.</summary>
    public void SendStopping()
    {
        try
        {
            var now = _time.GetUtcNow();
            _reporter.Submit(HeartbeatBuilder.Stopping(now, _options, Backlog(now)));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _logger.LogWarning("IdleMon stopping heartbeat failed ({Type}).", ex.GetType().Name);
        }
    }

    // The age of the oldest message in the spool; null when the spool is empty (the tag is then left out).
    private TimeSpan? Backlog(DateTimeOffset now)
    {
        try
        {
            return _spool?.OldestMessageTime() is { } oldest ? now - oldest : null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }
}
