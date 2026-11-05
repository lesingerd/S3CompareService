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
    private DateTimeOffset _nextMetrics;
    private DateTimeOffset _nextHeartbeat;
    private DateTimeOffset? _lastCollectionOk;
    private string? _databaseError;
    private IReadOnlyList<StuckScan> _stuckScans = [];
    private int _lastLoggedFailure;

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
        _nextMetrics = _startedAt;
        _nextHeartbeat = _startedAt;
    }

    /// <summary>Gets the alerts firing now.</summary>
    public IReadOnlyCollection<string> FiringAlerts => _alerts.FiringKeys;

    /// <summary>Does what is due now: a collection pass and/or a heartbeat.</summary>
    public async Task TickAsync(CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow();
        if (now >= _nextMetrics)
        {
            _nextMetrics = now + TimeSpan.FromSeconds(_options.MetricsIntervalSeconds);
            await CollectAsync(cancellationToken).ConfigureAwait(false);
        }

        // The heartbeat comes after the collection, so it reports the result of this very pass.
        now = _time.GetUtcNow();
        if (now >= _nextHeartbeat)
        {
            _nextHeartbeat = now + TimeSpan.FromSeconds(_options.HeartbeatIntervalSeconds);
            SendHeartbeat();
        }
    }

    /// <summary>Gets how long to wait until something is due.</summary>
    public TimeSpan DelayUntilNextTick()
    {
        var next = _nextMetrics < _nextHeartbeat ? _nextMetrics : _nextHeartbeat;
        var delay = next - _time.GetUtcNow();
        return delay < TimeSpan.FromMilliseconds(250) ? TimeSpan.FromMilliseconds(250) : delay;
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

            List<JobState> states;
            try
            {
                states = await ReadStatesAsync(active, timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException("the read timed out");
            }

            _progress.Observe(active, now);
            // Active scans are matched to the states read above; a scan that started meanwhile shows up on the next pass.
            var evaluation = AlertEvaluator.Evaluate(states, now, _options, _progress);

            foreach (var message in _alerts.Apply(evaluation.Conditions, now)) _reporter.Submit(message);
            foreach (var message in MetricBuilder.Build(states, active.Count, evaluation.OverdueJobs, now, _options, _results)) _reporter.Submit(message);
            _reporter.Submit(SnapshotBuilder.Jobs(states, now, _options));
            if (_options.IncludeObjectKeys)
            {
                foreach (var message in await TopDisparitiesAsync(states, now, cancellationToken).ConfigureAwait(false)) _reporter.Submit(message);
            }

            _lastCollectionOk = now;
            _databaseError = null;
            _stuckScans = evaluation.StuckScans;
            _lastLoggedFailure = 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Unknown is not zero: nothing is sent for this pass, alerts keep their state, and the heartbeat says degraded.
            _databaseError = ex is TimeoutException ? "timed out" : ex.GetType().Name;
            if (Interlocked.Exchange(ref _lastLoggedFailure, 1) == 0)
                _logger.LogWarning("IdleMon collection failed ({Type}); the heartbeat will say degraded until it works again.", ex.GetType().Name);
        }
    }

    private async Task<List<JobState>> ReadStatesAsync(IReadOnlyList<ActiveScanInfo> active, CancellationToken cancellationToken)
    {
        var jobs = await _jobs.GetAllAsync(cancellationToken).ConfigureAwait(false);
        var scansByJob = active.GroupBy(s => s.JobId).ToDictionary(g => g.Key, g => g.First());
        var states = new List<JobState>(jobs.Count);
        foreach (var job in jobs)
        {
            var runs = await _runs.GetRecentRunsAsync(RunsPerJob, job.Id, cancellationToken).ConfigureAwait(false);
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

    /// <summary>Sends one heartbeat from the state of the last collection.</summary>
    public void SendHeartbeat()
    {
        try
        {
            var now = _time.GetUtcNow();
            _reporter.Submit(HeartbeatBuilder.Build(new HeartbeatInput
            {
                Now = now,
                StartedAt = _startedAt,
                LastCollectionOk = _lastCollectionOk,
                DatabaseError = _databaseError,
                SchedulerLastEvaluation = _schedulerLastEvaluation(),
                StuckScans = _stuckScans,
                Backlog = Backlog(now),
            }, _options));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _logger.LogWarning("IdleMon heartbeat failed ({Type}).", ex.GetType().Name);
        }
    }

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
