using System.Diagnostics;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

/// <summary>
/// Splits the keyspace of a scan into key ranges that can be compared independently. S3 lists keys in UTF-8 binary order, so
/// any sorted set of boundary strings over the keys RELATIVE to each bucket's prefix (A and B prefixes may differ) cuts both
/// listings at the same places.
///
/// Boundary candidates are found in two steps, within a request budget and a time box:
/// <list type="number">
/// <item>Delimiter discovery: "/" listings under the prefix on both buckets; the union of the common prefixes is the candidate
/// set. It descends level by level (up to <c>maxDiscoveryDepth</c>) until there are at least <c>targetPartitions</c> candidates.</item>
/// <item>Probe fallback for flat or shallow namespaces, below the deepest "/" level found (or the whole keyspace): each base
/// first jumps to the longest prefix all its keys share (so <c>file_00001.dat</c> style names split where they differ),
/// then MaxKeys=1 listings with StartAfter = prefix + base + character find real keys as candidates, coarse to fine: evenly
/// spaced characters first, then one character deeper below the prefixes found (up to 4 rounds). The budget is split
/// evenly over rounds, bases and the two buckets (each probe goes to one bucket, alternating).</item>
/// </list>
/// The candidates are sorted and <c>targetPartitions - 1</c> of them, evenly spaced, become the boundaries. Correctness never
/// depends on the quality of the plan: <see cref="BuildRanges"/> always returns ranges that tile the whole keyspace exactly.
///
/// Robustness: every planner request has its own timeout (default 15 s, counted after the rate limiter and the concurrency
/// gate, so throttling is not a timeout). A request that fails or times out is logged as a warning and skipped; it never
/// aborts planning. If a shared-prefix check (StartAfter = p + U+10FFFF) fails, the jump is switched off for the rest of the
/// plan and probing continues. When <c>maxPlanningTime</c> is used up, no more requests are issued (those in flight are
/// cancelled) and the ranges are built from the candidates found so far. Only the scan's own cancellation propagates.
/// At most <see cref="MaxConcurrentPlannerRequests"/> requests are in flight at once, across everything the planner does.
/// </summary>
public sealed class PartitionPlanner
{
    /// <summary>The characters probed (space through tilde). Keys with other first characters land in the neighbouring range.</summary>
    public const string ProbeAlphabet = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

    /// <summary>The default time limit of one planner request.</summary>
    public static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(15);

    private const int MaxPagesPerPrefixListing = 10;
    /// <summary>Planner requests in flight at once (all buckets together). The per-bucket rate limiter still applies.</summary>
    public const int MaxConcurrentPlannerRequests = 16;
    private const int MaxProbeRounds = 4;
    private const int JumpFanout = 3;
    private const int MaxLoggedErrors = 20;

    /// <summary>Characters probed when the budget does not allow the whole alphabet (ASCII order).</summary>
    private const string CommonKeyChars = "-./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

    private readonly Side _a;
    private readonly Side _b;
    private readonly int _targetPartitions;
    private readonly int _maxDepth;
    private readonly int _maxRequests;
    private int _requestsUsed;
    private List<string>? _deepestLevel;
    private readonly Func<CancellationToken, Task>? _pauseGate;
    private readonly TimeSpan _maxPlanningTime;
    private readonly TimeSpan _requestTimeout;
    private readonly Action<string, string>? _log;
    private readonly ScanIssues? _issues;
    private readonly SemaphoreSlim _gate = new(MaxConcurrentPlannerRequests);
    private readonly CancellationTokenSource _deadline = new();
    private readonly Stopwatch _clock = new();

    // 0 = StartAfter marker U+10FFFF, 1 = fallback marker "~" (after the first failed), 2 = jump off (after the fallback failed too)
    private int _jumpState;
    private bool JumpOff => Volatile.Read(ref _jumpState) >= 2;
    private volatile bool _timedOut;
    private volatile string _step = string.Empty;
    private volatile string? _lastError;
    private volatile bool _started, _finished;
    private int _errors;
    private int _rangeCount;
    private SortedSet<string>? _liveCandidates;

    private sealed record Side(IS3Lister Lister, string Bucket, string Prefix, IRequestRateLimiter? Limiter, string Name);

    public PartitionPlanner(
        IS3Lister listerA, string bucketA, string? prefixA, IRequestRateLimiter? limiterA,
        IS3Lister listerB, string bucketB, string? prefixB, IRequestRateLimiter? limiterB,
        int targetPartitions,
        int maxDiscoveryDepth = ScanOptions.DefaultMaxDiscoveryDepth,
        int maxRequests = ScanOptions.DefaultMaxDiscoveryRequests,
        Func<CancellationToken, Task>? pauseGate = null,
        TimeSpan? maxPlanningTime = null,
        TimeSpan? requestTimeout = null,
        Action<string, string>? log = null,
        ScanIssues? issues = null)
    {
        _a = new Side(listerA ?? throw new ArgumentNullException(nameof(listerA)), bucketA, prefixA ?? string.Empty, limiterA, "A");
        _b = new Side(listerB ?? throw new ArgumentNullException(nameof(listerB)), bucketB, prefixB ?? string.Empty, limiterB, "B");
        _targetPartitions = Math.Max(1, targetPartitions);
        _maxDepth = Math.Max(0, maxDiscoveryDepth);
        _maxRequests = Math.Max(0, maxRequests);
        _pauseGate = pauseGate;
        _maxPlanningTime = maxPlanningTime is { } t && t > TimeSpan.Zero ? t : TimeSpan.Zero;
        _requestTimeout = requestTimeout is { } r && r > TimeSpan.Zero ? r : DefaultRequestTimeout;
        _log = log;
        _issues = issues;
    }

    /// <summary>S3 requests sent by <see cref="PlanAsync"/>.</summary>
    public int RequestsUsed => Math.Min(Volatile.Read(ref _requestsUsed), _maxRequests);

    /// <summary>How the boundaries were found ("none", "delimiter", "probe", "delimiter+probe"), for the log.</summary>
    public string Method { get; private set; } = "none";

    /// <summary>Distinct boundary candidates found by the last <see cref="PlanAsync"/>.</summary>
    public int CandidateCount { get; private set; }

    /// <summary>True when the planning time limit ended the search early.</summary>
    public bool TimedOut => _timedOut;

    /// <summary>True when the shared-prefix jump was switched off after failed requests (see <see cref="JumpMode"/>).</summary>
    public bool JumpDisabled => JumpOff;

    /// <summary>"U+10FFFF" (normal), "~ fallback" (the first marker was rejected) or "disabled".</summary>
    public string JumpMode => Volatile.Read(ref _jumpState) switch { 0 => "U+10FFFF", 1 => "~ fallback", _ => "disabled" };

    /// <summary>Planner requests that failed or timed out (the time limit cancelling a request does not count).</summary>
    public int RequestErrors => Volatile.Read(ref _errors);

    /// <summary>The most recent request error, or null.</summary>
    public string? LastError => _lastError;

    /// <summary>Live numbers for the banner and diagnostics panel. Safe to call from any thread at any time.</summary>
    public PlannerStats GetStats() => new()
    {
        Started = _started,
        Finished = _finished,
        RequestsUsed = RequestsUsed,
        RequestBudget = _maxRequests,
        Candidates = _finished ? CandidateCount : LiveCandidateCount(),
        TargetPartitions = _targetPartitions,
        Step = _step,
        ElapsedSeconds = _clock.Elapsed.TotalSeconds,
        TimeLimitSeconds = _maxPlanningTime.TotalSeconds,
        TimedOut = _timedOut,
        JumpDisabled = JumpOff,
        JumpMode = JumpMode,
        RequestErrors = RequestErrors,
        LastError = _lastError,
        Method = Method,
        Ranges = _rangeCount
    };

    private int LiveCandidateCount()
    {
        var set = _liveCandidates;
        if (set == null) return 0;
        lock (set) return set.Count;
    }

    private bool TimeUp => _deadline.IsCancellationRequested;

    private bool TryTakeRequest()
    {
        if (TimeUp) { _timedOut = true; return false; }
        return Interlocked.Increment(ref _requestsUsed) <= _maxRequests;
    }

    private void SetStep(string step)
    {
        if (_step == step) return;
        _step = step;
        _log?.Invoke("INFO", $"Planner step: {step} ({RequestsUsed}/{_maxRequests} requests, {LiveCandidateCount()} candidates, {_clock.Elapsed.TotalSeconds:0} s).");
    }

    /// <summary>Finds boundaries and returns the ranges (one range when no planning is wanted or possible).</summary>
    public async Task<List<PartitionRange>> PlanAsync(CancellationToken cancellationToken)
    {
        _started = true;
        _clock.Restart();
        if (_targetPartitions <= 1 || _maxRequests <= 0)
        {
            CandidateCount = 0;
            _finished = true;
            _rangeCount = 1;
            return BuildRanges(Array.Empty<string>(), 1);
        }

        if (_maxPlanningTime > TimeSpan.Zero) _deadline.CancelAfter(_maxPlanningTime);
        var candidates = new SortedSet<string>(Utf8OrdinalComparer.Instance);
        _liveCandidates = candidates;
        try
        {
            bool usedDelimiter = await DiscoverByDelimiterAsync(candidates, cancellationToken).ConfigureAwait(false);
            bool usedProbe = false;
            if (LiveCandidateCount() < _targetPartitions && !TimeUp)
            {
                usedProbe = await ProbeAsync(candidates, cancellationToken).ConfigureAwait(false);
            }
            if (TimeUp) _timedOut = true;

            Method = (usedDelimiter, usedProbe) switch
            {
                (true, true) => "delimiter+probe",
                (true, false) => "delimiter",
                (false, true) => "probe",
                _ => "none"
            };
            List<PartitionRange> ranges;
            lock (candidates)
            {
                CandidateCount = candidates.Count;
                ranges = BuildRanges(candidates.ToList(), _targetPartitions);
            }
            _rangeCount = ranges.Count;
            if (_errors > MaxLoggedErrors) _log?.Invoke("WARN", $"Planner: {_errors - MaxLoggedErrors} further skipped request(s) were not logged individually ({_errors} in total).");
            if (_timedOut)
            {
                string degraded = $"planner time limit of {_maxPlanningTime.TotalSeconds:0} s reached — using the {CandidateCount} candidate range boundaries found so far ({RequestsUsed}/{_maxRequests} requests); the scan is still complete and results are unaffected, only the range balance (speed) may be worse";
                _issues?.Record(IssueOutcome.Degraded, degraded, log: false);
                _log?.Invoke("WARN", $"{ScanIssues.Label(IssueOutcome.Degraded)}: Planner: the {_maxPlanningTime.TotalSeconds:0} s planning time limit was reached; using the {CandidateCount} candidate(s) found so far ({RequestsUsed}/{_maxRequests} requests). The scan is still complete and results are unaffected, only the range balance (speed) may be worse.");
            }
            return ranges;
        }
        finally
        {
            _finished = true;
            _clock.Stop();
        }
    }

    // ---------------- request plumbing ----------------

    /// <summary>
    /// One planner list request with its own timeout. Returns null when it failed, timed out, or the time limit cancelled it;
    /// throws only when the scan itself is cancelled. The caller has taken the budget.
    /// </summary>
    private async Task<S3ListPage?> RequestAsync(Side side, string? prefix, string? startAfter, string? token, string? delimiter, int maxKeys, int jumpState, string what, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, _deadline.Token);
        bool gateHeld = false;
        try
        {
            if (_pauseGate != null) await _pauseGate(ct).ConfigureAwait(false);
            await _gate.WaitAsync(cts.Token).ConfigureAwait(false);
            gateHeld = true;
            if (side.Limiter != null) await side.Limiter.WaitAsync(cts.Token).ConfigureAwait(false);
            // The timeout starts here, after any throttling or queueing.
            cts.CancelAfter(_requestTimeout);
            return await side.Lister.ListPageAsync(side.Bucket, prefix, startAfter, token, delimiter, maxKeys, cts.Token)
                .WaitUnlessDoneAsync(_requestTimeout, cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ct.ThrowIfCancellationRequested();
            if (TimeUp)
            {
                _timedOut = true; // cut short by the time limit: not an error
                return null;
            }
            string reason = ex is TimeoutException or OperationCanceledException
                ? $"timed out after {_requestTimeout.TotalSeconds:0} s"
                : $"{ex.GetType().Name}: {ex.Message}";
            ReportError(side, what, reason, jumpState);
            return null;
        }
        finally
        {
            if (gateHeld) _gate.Release();
        }
    }

    private void ReportError(Side side, string what, string reason, int jumpState)
    {
        int n = Interlocked.Increment(ref _errors);
        string msg = $"planner request bucket {side.Name} {what}: {reason} — planning continued; the plan is still complete and results are unaffected (only split quality may be lower)";
        string jumpNote = string.Empty;
        if (jumpState >= 0 && Interlocked.CompareExchange(ref _jumpState, jumpState + 1, jumpState) == jumpState)
        {
            jumpNote = jumpState == 0
                ? $"Planner: the shared-prefix check (StartAfter = prefix + U+10FFFF) failed on bucket {side.Name} ({reason}); falling back to the '~' marker for the rest of this plan."
                : $"Planner: the shared-prefix check also failed with the '~' marker on bucket {side.Name} ({reason}); the jump is disabled for the rest of this plan and probing continues without it.";
            msg += jumpState == 0 ? "; shared-prefix jump fell back to the '~' marker" : "; shared-prefix jump disabled for this plan";
        }
        _lastError = ScanIssues.Label(IssueOutcome.Skipped) + ": " + msg;
        _issues?.Record(IssueOutcome.Skipped, msg, log: false);
        if (n <= MaxLoggedErrors) _log?.Invoke("WARN", $"{_lastError}.");
        if (jumpNote.Length > 0) _log?.Invoke("WARN", jumpNote);
    }

    // ---------------- 1. delimiter discovery ----------------

    private async Task<bool> DiscoverByDelimiterAsync(SortedSet<string> candidates, CancellationToken ct)
    {
        bool any = false;
        var level = new List<string> { string.Empty };
        for (int depth = 1; depth <= _maxDepth && level.Count > 0 && !TimeUp; depth++)
        {
            SetStep($"delimiter depth {depth}");
            var found = new SortedSet<string>(Utf8OrdinalComparer.Instance);
            var gate = new object();
            await Parallel.ForEachAsync(level, new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentPlannerRequests, CancellationToken = ct }, async (rel, token) =>
            {
                var both = await Task.WhenAll(
                    CommonPrefixesAsync(_a, rel, token),
                    CommonPrefixesAsync(_b, rel, token)).ConfigureAwait(false);
                lock (gate)
                {
                    foreach (var list in both) found.UnionWith(list);
                }
            }).ConfigureAwait(false);

            if (found.Count == 0) break;
            any = true;
            lock (candidates) candidates.UnionWith(found);
            _deepestLevel = found.ToList();
            if (LiveCandidateCount() >= _targetPartitions || RequestsUsed >= _maxRequests) break;
            level = _deepestLevel;
        }
        return any;
    }

    /// <summary>Common prefixes directly below <paramref name="relativePrefix"/>, relative to the bucket prefix.</summary>
    private async Task<List<string>> CommonPrefixesAsync(Side side, string relativePrefix, CancellationToken ct)
    {
        var result = new List<string>();
        string listPrefix = side.Prefix + relativePrefix;
        string? token = null;
        for (int page = 0; page < MaxPagesPerPrefixListing; page++)
        {
            if (!TryTakeRequest()) break;
            var response = await RequestAsync(side, listPrefix, null, token, "/", 1000, -1, $"delimiter listing prefix='{InstrumentedLister.Clip(listPrefix)}' page {page + 1}", ct).ConfigureAwait(false);
            if (response == null) break;
            foreach (var cp in response.CommonPrefixes)
            {
                if (cp.StartsWith(side.Prefix, StringComparison.Ordinal)) result.Add(cp.Substring(side.Prefix.Length));
            }
            if (!response.IsTruncated || string.IsNullOrEmpty(response.NextContinuationToken)) break;
            // A full first page of plain objects and no folder at all: a flat namespace. Paging on would cost up to 9 more
            // sequential requests (9 s at 1 s latency) for nothing in the usual case, so stop; probing splits flat namespaces.
            if (page == 0 && response.CommonPrefixes.Count == 0) break;
            token = response.NextContinuationToken;
        }
        return result;
    }

    // ---------------- 2. probe fallback ----------------

    /// <summary>
    /// A key prefix whose keys are to be split further: the first key known under it (null = not looked up yet) and which
    /// buckets have keys under it.
    /// </summary>
    private sealed record ProbeBase(string Prefix, string? FirstKey, bool OnA, bool OnB);

    /// <summary>First key after a probe. Unknown = the request failed, timed out or got no budget: "no key" is then not proven.</summary>
    private readonly record struct ProbeResult(string? Key, bool Unknown);

    /// <summary>
    /// Probes within the budget, coarse to fine. The bases are the deepest "/" level discovery found (or the start of the
    /// keyspace). Each base first jumps to the longest prefix all of its keys share (cheap, not a probe round), so names
    /// like <c>file_00001.dat</c> or <c>logs-2024-01-01</c> are split where they actually differ. Then each round probes
    /// evenly spaced characters after every base (MaxKeys=1, StartAfter = prefix + base + char, one bucket per probe,
    /// alternating), and the next round refines one character below the prefixes it found (again jumping over shared
    /// prefixes). Each round may spend half of the remaining budget (the last round all of it), shared evenly by its bases.
    /// All probes of a round run concurrently (bounded by the planner's concurrency gate and the rate limiters).
    /// </summary>
    private async Task<bool> ProbeAsync(SortedSet<string> candidates, CancellationToken ct)
    {
        int before = LiveCandidateCount();
        var roots = _deepestLevel is { Count: > 0 } level ? level.ToList() : new List<string> { string.Empty };
        int maxRoots = Math.Max(1, Remaining / 4);
        if (roots.Count > maxRoots) roots = EvenlySpaced(roots, maxRoots);

        // Shared-prefix jump for the roots: at most half of the budget, split evenly.
        SetStep("shared-prefix jump");
        int jumpBudget = Math.Max(2, Remaining / (2 * roots.Count));
        var bases = await JumpAllAsync(roots.Select(r => new ProbeBase(r, null, true, true)).ToList(), jumpBudget, candidates, ct).ConfigureAwait(false);

        for (int round = 0; round < MaxProbeRounds && bases.Count > 0 && LiveCandidateCount() < _targetPartitions && !TimeUp; round++)
        {
            int remaining = Remaining;
            if (remaining <= 0) break;
            SetStep($"probe round {round + 1}");
            int roundsLeft = MaxProbeRounds - round;
            int spend = roundsLeft == 1 ? remaining : Math.Max(1, remaining / 2);
            if (bases.Count > spend) bases = EvenlySpaced(bases, spend);
            int perBase = Math.Max(1, spend / bases.Count);

            var probes = new List<(ProbeBase Base, string Probe, Side Side)>();
            for (int bi = 0; bi < bases.Count; bi++)
            {
                var b = bases[bi];
                var chars = ProbeChars(MinNextChar(b), perBase);
                for (int i = 0; i < chars.Count; i++) probes.Add((b, b.Prefix + chars[i], PickSide(b, bi + i)));
            }
            if (probes.Count == 0) break;

            var found = await RunProbesAsync(probes, ct).ConfigureAwait(false);
            lock (candidates) candidates.UnionWith(found.Select(f => f.Key));

            // Next round: one character below every prefix that has keys (the probe hits and each base's first key), keeping
            // the first key seen under each new prefix.
            var next = new Dictionary<string, ProbeBase>(StringComparer.Ordinal);
            var seen = found.Concat(bases.Where(b => b.FirstKey != null).Select(b => (Base: b, Key: b.FirstKey!)));
            foreach (var (b, key) in seen)
            {
                if (!key.StartsWith(b.Prefix, StringComparison.Ordinal)) continue;
                int cut = CutAfter(key, b.Prefix.Length);
                if (key.Length <= cut) continue; // the key is the prefix itself: nothing below it to split
                string sub = key.Substring(0, cut);
                if (!next.TryGetValue(sub, out var existing) || Utf8OrdinalComparer.CompareStatic(key, existing.FirstKey) < 0)
                    next[sub] = new ProbeBase(sub, key, b.OnA, b.OnB);
            }
            bases = next.Values.OrderBy(p => p.Prefix, Utf8OrdinalComparer.Instance).ToList();

            // Jump over shared prefixes below the new bases too, when the budget easily allows it (a check costs 1-2 requests).
            if (bases.Count > 0 && !JumpOff && !TimeUp && Remaining >= bases.Count * 8)
            {
                SetStep($"shared-prefix jump (after round {round + 1})");
                bases = await JumpAllAsync(bases, Math.Max(2, Remaining / (4 * bases.Count)), candidates, ct).ConfigureAwait(false);
            }
        }
        return LiveCandidateCount() > before;
    }

    private int Remaining => Math.Max(0, _maxRequests - Volatile.Read(ref _requestsUsed));

    private async Task<List<ProbeBase>> JumpAllAsync(List<ProbeBase> bases, int budgetEach, SortedSet<string> candidates, CancellationToken ct)
    {
        var result = new List<ProbeBase>();
        var gate = new object();
        await Parallel.ForEachAsync(bases, new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentPlannerRequests, CancellationToken = ct }, async (b, token) =>
        {
            var jumped = await JumpAsync(b, budgetEach, candidates, token).ConfigureAwait(false);
            if (jumped != null) lock (gate) result.Add(jumped);
        }).ConfigureAwait(false);
        return result.OrderBy(p => p.Prefix, Utf8OrdinalComparer.Instance).ToList();
    }

    /// <summary>
    /// Extends <paramref name="b"/> to the longest prefix that every key under it shares (in both buckets), using at most
    /// <paramref name="budget"/> requests. Looks up the first key under the base if not known (null result: no keys at all).
    /// A prefix p is shared when the first key after p + U+10FFFF (the highest code point, so past every key starting with
    /// p) no longer starts with the base. The shortest extension is tested first, so a base without a long shared prefix
    /// costs one check; otherwise a search over the first key's length finds the longest one. Both buckets are asked at the
    /// same time, and the search tests up to <see cref="JumpFanout"/> lengths per step concurrently, so a slow endpoint costs
    /// few round trips in sequence.
    /// </summary>
    private async Task<ProbeBase?> JumpAsync(ProbeBase b, int budget, SortedSet<string> candidates, CancellationToken ct)
    {
        int used = 0;
        bool Take() => Interlocked.Increment(ref used) <= budget;
        void Add(string key) { lock (candidates) candidates.Add(key); }

        string? first = b.FirstKey;
        bool onA = b.OnA, onB = b.OnB;
        string common;
        if (first == null)
        {
            var ta = onA && Take() ? ProbeOneAsync(_a, b.Prefix, -1, ct) : Task.FromResult(new ProbeResult(null, onA));
            var tb = onB && Take() ? ProbeOneAsync(_b, b.Prefix, -1, ct) : Task.FromResult(new ProbeResult(null, onB));
            await Task.WhenAll(ta, tb).ConfigureAwait(false);
            string? fa = ta.Result.Key, fb = tb.Result.Key;
            bool unknown = ta.Result.Unknown || tb.Result.Unknown;
            if (fa != null && !fa.StartsWith(b.Prefix, StringComparison.Ordinal)) { Add(fa); fa = null; }
            if (fb != null && !fb.StartsWith(b.Prefix, StringComparison.Ordinal)) { Add(fb); fb = null; }
            if (fa == null && fb == null) return unknown || Remaining <= 0 ? b : null; // no keys under it (or not proven / no budget)
            onA = fa != null;
            onB = fb != null;
            if (fa != null) Add(fa);
            if (fb != null) Add(fb);
            first = fa == null ? fb! : fb == null ? fa : Utf8OrdinalComparer.CompareStatic(fa, fb) <= 0 ? fa : fb;
            common = fa == null ? fb! : fb == null ? fa : CommonPrefix(fa, fb);
        }
        else
        {
            common = first;
        }

        // Valid cut positions beyond the base (never between the two halves of a surrogate pair).
        var cuts = new List<int>();
        for (int k = b.Prefix.Length + 1; k <= common.Length; k++)
        {
            if (!char.IsHighSurrogate(common[k - 1])) cuts.Add(k);
        }

        int best = b.Prefix.Length;
        if (cuts.Count > 0 && !JumpOff)
        {
            // true = shared, false = not shared, null = unknown (failed, no budget, jump disabled).
            async Task<bool?> SharedAsync(int length)
            {
                int state = Volatile.Read(ref _jumpState);
                if (state >= 2) return null;
                string marker = state == 0 ? MaxCodePoint : "~";
                string p = common.Substring(0, length);
                var checks = new List<Task<ProbeResult>>(2);
                bool incomplete = false;
                foreach (var (side, on) in new[] { (_a, onA), (_b, onB) })
                {
                    if (!on) continue;
                    if (Take()) checks.Add(ProbeOneAsync(side, p + marker, state, ct));
                    else incomplete = true;
                }
                var results = await Task.WhenAll(checks).ConfigureAwait(false);
                bool notShared = false;
                foreach (var r in results)
                {
                    if (r.Key != null)
                    {
                        Add(r.Key);
                        if (r.Key.StartsWith(b.Prefix, StringComparison.Ordinal)) notShared = true;
                    }
                    if (r.Unknown) incomplete = true;
                }
                if (notShared) return false;
                return incomplete ? null : true;
            }

            if (await SharedAsync(cuts[0]).ConfigureAwait(false) == true)
            {
                int lo = 0, hi = cuts.Count - 1; // cuts[lo] is shared; find the last shared cut
                bool stop = false;
                while (lo < hi && !stop)
                {
                    int span = hi - lo;
                    int k = Math.Min(JumpFanout, span);
                    var points = new List<int>(k);
                    for (int i = 1; i <= k; i++)
                    {
                        int p = lo + (int)Math.Ceiling(span * i / (double)(k + 1));
                        if (p > lo && p <= hi && (points.Count == 0 || p > points[^1])) points.Add(p);
                    }
                    var answers = await Task.WhenAll(points.Select(p => SharedAsync(cuts[p]))).ConfigureAwait(false);
                    for (int i = 0; i < points.Count; i++)
                    {
                        if (answers[i] == null) { stop = true; break; }
                        if (answers[i] == true) lo = points[i];
                        else { hi = points[i] - 1; break; }
                    }
                }
                best = cuts[lo];
            }
        }

        return new ProbeBase(common.Substring(0, best), first, onA, onB);
    }

    /// <summary>The highest Unicode code point (UTF-8 F4 8F BF BF): p + this sorts after every key that starts with p.</summary>
    private const string MaxCodePoint = "\U0010FFFF";

    private static string CommonPrefix(string x, string y)
    {
        int n = Math.Min(x.Length, y.Length), i = 0;
        while (i < n && x[i] == y[i]) i++;
        if (i > 0 && char.IsHighSurrogate(x[i - 1])) i--; // do not split a surrogate pair
        return x.Substring(0, i);
    }

    /// <summary>The length of the prefix of <paramref name="key"/> that is one character (code point) longer than <paramref name="length"/>.</summary>
    private static int CutAfter(string key, int length)
    {
        int cut = length + 1;
        if (cut < key.Length && char.IsHighSurrogate(key[cut - 1])) cut++;
        return Math.Min(cut, key.Length);
    }

    /// <summary>The character after the base in its first key: probing at or below it finds nothing new.</summary>
    private static char? MinNextChar(ProbeBase b) =>
        b.FirstKey != null && b.FirstKey.Length > b.Prefix.Length && b.FirstKey.StartsWith(b.Prefix, StringComparison.Ordinal)
            ? b.FirstKey[b.Prefix.Length]
            : null;

    private Side PickSide(ProbeBase b, int index)
    {
        if (b.OnA && b.OnB) return index % 2 == 0 ? _a : _b;
        return b.OnB ? _b : _a;
    }

    /// <summary>
    /// Up to <paramref name="count"/> probe characters above <paramref name="after"/>: the whole printable alphabet when the
    /// budget allows, otherwise the common key characters (digits, letters, - . / _) evenly spaced.
    /// </summary>
    private static List<char> ProbeChars(char? after, int count)
    {
        var alphabet = ProbeAlphabet.Where(c => after == null || c > after.Value).ToList();
        if (count >= alphabet.Count) return alphabet;
        var common = CommonKeyChars.Where(c => after == null || c > after.Value).ToList();
        if (common.Count == 0) common = alphabet;
        return EvenlySpacedCentered(common, Math.Min(count, common.Count));
    }

    private static List<T> EvenlySpaced<T>(List<T> items, int count)
    {
        if (count >= items.Count) return items;
        var list = new List<T>(count);
        for (int i = 0; i < count; i++) list.Add(items[(int)((long)i * items.Count / count)]);
        return list;
    }

    /// <summary>Evenly spaced, each pick in the middle of its share (so a single pick is the middle item, not the first).</summary>
    private static List<T> EvenlySpacedCentered<T>(List<T> items, int count)
    {
        if (count >= items.Count) return items;
        var list = new List<T>(count);
        for (int i = 0; i < count; i++) list.Add(items[(int)((2L * i + 1) * items.Count / (2L * count))]);
        return list;
    }

    private async Task<List<(ProbeBase Base, string Key)>> RunProbesAsync(List<(ProbeBase Base, string Probe, Side Side)> probes, CancellationToken ct)
    {
        var found = new List<(ProbeBase Base, string Key)>();
        var gate = new object();
        await Parallel.ForEachAsync(probes, new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentPlannerRequests, CancellationToken = ct }, async (p, token) =>
        {
            var r = await ProbeOneAsync(p.Side, p.Probe, -1, token).ConfigureAwait(false);
            if (r.Key != null) lock (gate) found.Add((p.Base, r.Key));
        }).ConfigureAwait(false);
        return found;
    }

    /// <summary>The first relative key after prefix + probe (Key null = none; Unknown when the request failed or had no budget).</summary>
    private async Task<ProbeResult> ProbeOneAsync(Side side, string probe, int jumpState, CancellationToken ct)
    {
        if (!TryTakeRequest()) return new ProbeResult(null, true);
        var page = await RequestAsync(side, side.Prefix, side.Prefix + probe, null, null, 1, jumpState,
            $"{(jumpState >= 0 ? "shared-prefix check" : "probe")} startAfter='{InstrumentedLister.Clip(side.Prefix + probe)}'", ct).ConfigureAwait(false);
        if (page == null) return new ProbeResult(null, true);
        var key = page.Objects.FirstOrDefault()?.Key;
        if (key == null || !key.StartsWith(side.Prefix, StringComparison.Ordinal)) return new ProbeResult(null, false);
        return new ProbeResult(key.Substring(side.Prefix.Length), false);
    }

    /// <summary>
    /// The plan for resuming a single-key (version 1) checkpoint: the range (start, <paramref name="done"/>] is finished, and
    /// the planned ranges after it follow, the first one starting at <paramref name="done"/>. Tiles the keyspace exactly.
    /// </summary>
    public static List<PartitionRange> ResumePlanAfter(IReadOnlyList<PartitionRange> plan, string done)
    {
        var result = new List<PartitionRange> { new() { Lower = null, Upper = done, LastCompletedKey = done, Completed = true } };
        foreach (var r in plan)
        {
            if (r.Upper != null && Utf8OrdinalComparer.CompareStatic(r.Upper, done) <= 0) continue;
            var c = r.Clone();
            if (result.Count == 1) c.Lower = done;
            c.LastCompletedKey = null;
            c.Completed = false;
            result.Add(c);
        }
        return result;
    }

    // ---------------- range construction ----------------

    /// <summary>
    /// Turns boundary candidates into at most <paramref name="targetPartitions"/> ranges: (null, b1], (b1, b2], ..., (bk, null),
    /// with the k = targetPartitions - 1 boundaries spaced evenly over the sorted, distinct, non-empty candidates. Works for any
    /// input (no candidates = one range), and the result always tiles the keyspace exactly.
    /// </summary>
    public static List<PartitionRange> BuildRanges(IEnumerable<string> candidates, int targetPartitions)
    {
        var sorted = candidates
            .Where(c => !string.IsNullOrEmpty(c))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(c => c, Utf8OrdinalComparer.Instance)
            .ToList();

        int boundaryCount = Math.Min(Math.Max(0, targetPartitions - 1), sorted.Count);
        var boundaries = new List<string>(boundaryCount);
        for (int i = 1; i <= boundaryCount; i++)
        {
            string b = sorted[(int)((long)i * sorted.Count / (boundaryCount + 1))];
            if (boundaries.Count == 0 || Utf8OrdinalComparer.CompareStatic(boundaries[^1], b) < 0) boundaries.Add(b);
        }

        var ranges = new List<PartitionRange>(boundaries.Count + 1);
        string? lower = null;
        foreach (var b in boundaries)
        {
            ranges.Add(new PartitionRange { Lower = lower, Upper = b });
            lower = b;
        }
        ranges.Add(new PartitionRange { Lower = lower, Upper = null });
        return ranges;
    }

    /// <summary>
    /// Checks that <paramref name="ranges"/> tile the keyspace exactly: the first starts at the beginning, the last ends at the
    /// end, each range starts where the previous one ended, bounds increase strictly, and recorded progress lies inside its range.
    /// Returns an error message, or null when valid.
    /// </summary>
    public static string? ValidateTiling(IReadOnlyList<PartitionRange>? ranges)
    {
        if (ranges == null || ranges.Count == 0) return "the plan has no ranges";
        if (ranges[0].Lower != null) return "the first range does not start at the beginning of the keyspace";
        if (ranges[^1].Upper != null) return "the last range does not end at the end of the keyspace";
        for (int i = 0; i < ranges.Count; i++)
        {
            var r = ranges[i];
            if (i < ranges.Count - 1)
            {
                if (r.Upper == null) return $"range {i} ends at the end of the keyspace but is not the last";
                if (!string.Equals(r.Upper, ranges[i + 1].Lower, StringComparison.Ordinal)) return $"ranges {i} and {i + 1} do not meet (gap or overlap)";
            }
            if (r.Lower != null && r.Upper != null && Utf8OrdinalComparer.CompareStatic(r.Lower, r.Upper) >= 0)
                return $"range {i} is empty or reversed";
            if (r.LastCompletedKey != null)
            {
                if (r.Lower != null && Utf8OrdinalComparer.CompareStatic(r.LastCompletedKey, r.Lower) <= 0) return $"range {i} progress lies before the range";
                if (r.Upper != null && Utf8OrdinalComparer.CompareStatic(r.LastCompletedKey, r.Upper) > 0) return $"range {i} progress lies after the range";
            }
        }
        return null;
    }

    /// <summary>Index of the range that contains <paramref name="relativeKey"/> (binary search over a valid tiling).</summary>
    public static int IndexOf(IReadOnlyList<PartitionRange> ranges, string relativeKey)
    {
        int lo = 0, hi = ranges.Count - 1;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            var upper = ranges[mid].Upper;
            if (upper != null && Utf8OrdinalComparer.CompareStatic(relativeKey, upper) > 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /// <summary>
    /// The frontier of a plan: every relative key up to it is done. For one range this is simply its last completed key. When
    /// every range is complete it is the highest completed key. Has = false when nothing at the start of the keyspace is done.
    /// </summary>
    public static (bool Has, string Key) Frontier(IReadOnlyList<PartitionRange> ranges)
    {
        string? min = null;
        bool anyOpen = false;
        foreach (var r in ranges)
        {
            if (r.Completed) continue;
            anyOpen = true;
            var k = r.ResumeAfter;
            if (k == null) return (false, string.Empty);
            if (min == null || Utf8OrdinalComparer.CompareStatic(k, min) < 0) min = k;
        }
        if (anyOpen) return (true, min!);

        string? max = null;
        foreach (var r in ranges)
        {
            if (r.LastCompletedKey != null && (max == null || Utf8OrdinalComparer.CompareStatic(r.LastCompletedKey, max) > 0)) max = r.LastCompletedKey;
        }
        return (max != null, max ?? string.Empty);
    }
}
