using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

/// <summary>
/// Splits the keyspace of a scan into key ranges that can be compared independently. S3 lists keys in UTF-8 binary order, so
/// any sorted set of boundary strings over the keys RELATIVE to each bucket's prefix (A and B prefixes may differ) cuts both
/// listings at the same places.
///
/// Boundary candidates are found in two steps, within a request budget:
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
/// </summary>
public sealed class PartitionPlanner
{
    /// <summary>The characters probed (space through tilde). Keys with other first characters land in the neighbouring range.</summary>
    public const string ProbeAlphabet = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

    private const int MaxPagesPerPrefixListing = 10;
    private const int MaxConcurrentPlannerRequests = 8;
    private const int MaxProbeRounds = 4;


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

    private sealed record Side(IS3Lister Lister, string Bucket, string Prefix, IRequestRateLimiter? Limiter);

    public PartitionPlanner(
        IS3Lister listerA, string bucketA, string? prefixA, IRequestRateLimiter? limiterA,
        IS3Lister listerB, string bucketB, string? prefixB, IRequestRateLimiter? limiterB,
        int targetPartitions,
        int maxDiscoveryDepth = ScanOptions.DefaultMaxDiscoveryDepth,
        int maxRequests = ScanOptions.DefaultMaxDiscoveryRequests,
        Func<CancellationToken, Task>? pauseGate = null)
    {
        _a = new Side(listerA ?? throw new ArgumentNullException(nameof(listerA)), bucketA, prefixA ?? string.Empty, limiterA);
        _b = new Side(listerB ?? throw new ArgumentNullException(nameof(listerB)), bucketB, prefixB ?? string.Empty, limiterB);
        _targetPartitions = Math.Max(1, targetPartitions);
        _maxDepth = Math.Max(0, maxDiscoveryDepth);
        _maxRequests = Math.Max(0, maxRequests);
        _pauseGate = pauseGate;
    }

    /// <summary>S3 requests sent by <see cref="PlanAsync"/>.</summary>
    public int RequestsUsed => Math.Min(Volatile.Read(ref _requestsUsed), _maxRequests);

    /// <summary>How the boundaries were found ("none", "delimiter", "probe", "delimiter+probe"), for the log.</summary>
    public string Method { get; private set; } = "none";

    /// <summary>Distinct boundary candidates found by the last <see cref="PlanAsync"/>.</summary>
    public int CandidateCount { get; private set; }

    private bool TryTakeRequest() => Interlocked.Increment(ref _requestsUsed) <= _maxRequests;

    /// <summary>Finds boundaries and returns the ranges (one range when no planning is wanted or possible).</summary>
    public async Task<List<PartitionRange>> PlanAsync(CancellationToken cancellationToken)
    {
        if (_targetPartitions <= 1 || _maxRequests <= 0)
        {
            CandidateCount = 0;
            return BuildRanges(Array.Empty<string>(), 1);
        }

        var candidates = new SortedSet<string>(Utf8OrdinalComparer.Instance);
        bool usedDelimiter = await DiscoverByDelimiterAsync(candidates, cancellationToken).ConfigureAwait(false);
        bool usedProbe = false;
        if (candidates.Count < _targetPartitions)
        {
            usedProbe = await ProbeAsync(candidates, cancellationToken).ConfigureAwait(false);
        }

        Method = (usedDelimiter, usedProbe) switch
        {
            (true, true) => "delimiter+probe",
            (true, false) => "delimiter",
            (false, true) => "probe",
            _ => "none"
        };
        CandidateCount = candidates.Count;
        return BuildRanges(candidates, _targetPartitions);
    }

    // ---------------- 1. delimiter discovery ----------------

    private async Task<bool> DiscoverByDelimiterAsync(SortedSet<string> candidates, CancellationToken ct)
    {
        bool any = false;
        var level = new List<string> { string.Empty };
        for (int depth = 1; depth <= _maxDepth && level.Count > 0; depth++)
        {
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
            candidates.UnionWith(found);
            _deepestLevel = found.ToList();
            if (candidates.Count >= _targetPartitions || RequestsUsed >= _maxRequests) break;
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
            await BeforeRequestAsync(side, ct).ConfigureAwait(false);
            var response = await side.Lister.ListPageAsync(side.Bucket, listPrefix, null, token, "/", 1000, ct).ConfigureAwait(false);
            foreach (var cp in response.CommonPrefixes)
            {
                if (cp.StartsWith(side.Prefix, StringComparison.Ordinal)) result.Add(cp.Substring(side.Prefix.Length));
            }
            if (!response.IsTruncated || string.IsNullOrEmpty(response.NextContinuationToken)) break;
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

    /// <summary>
    /// Probes within the budget, coarse to fine. The bases are the deepest "/" level discovery found (or the start of the
    /// keyspace). Each base first jumps to the longest prefix all of its keys share (cheap, not a probe round), so names
    /// like <c>file_00001.dat</c> or <c>logs-2024-01-01</c> are split where they actually differ. Then each round probes
    /// evenly spaced characters after every base (MaxKeys=1, StartAfter = prefix + base + char, one bucket per probe,
    /// alternating), and the next round refines one character below the prefixes it found (again jumping over shared
    /// prefixes). Each round may spend half of the remaining budget (the last round all of it), shared evenly by its bases.
    /// </summary>
    private async Task<bool> ProbeAsync(SortedSet<string> candidates, CancellationToken ct)
    {
        int before = candidates.Count;
        var roots = _deepestLevel is { Count: > 0 } level ? level.ToList() : new List<string> { string.Empty };
        int maxRoots = Math.Max(1, Remaining / 4);
        if (roots.Count > maxRoots) roots = EvenlySpaced(roots, maxRoots);

        // Shared-prefix jump for the roots: at most half of the budget, split evenly.
        int jumpBudget = Math.Max(2, Remaining / (2 * roots.Count));
        var bases = await JumpAllAsync(roots.Select(r => new ProbeBase(r, null, true, true)).ToList(), jumpBudget, candidates, ct).ConfigureAwait(false);

        for (int round = 0; round < MaxProbeRounds && bases.Count > 0 && candidates.Count < _targetPartitions; round++)
        {
            int remaining = Remaining;
            if (remaining <= 0) break;
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
            if (bases.Count > 0 && Remaining >= bases.Count * 8)
            {
                bases = await JumpAllAsync(bases, Math.Max(2, Remaining / (4 * bases.Count)), candidates, ct).ConfigureAwait(false);
            }
        }
        return candidates.Count > before;
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
    /// costs one check; otherwise a binary search over the first key's length finds the longest one.
    /// </summary>
    private async Task<ProbeBase?> JumpAsync(ProbeBase b, int budget, SortedSet<string> candidates, CancellationToken ct)
    {
        int used = 0;
        bool Take() => used++ < budget;
        void Add(string key) { lock (candidates) candidates.Add(key); }

        string? first = b.FirstKey;
        bool onA = b.OnA, onB = b.OnB;
        string common;
        if (first == null)
        {
            string? fa = onA && Take() ? await ProbeOneAsync(_a, b.Prefix, ct).ConfigureAwait(false) : null;
            string? fb = onB && Take() ? await ProbeOneAsync(_b, b.Prefix, ct).ConfigureAwait(false) : null;
            if (fa != null && !fa.StartsWith(b.Prefix, StringComparison.Ordinal)) { Add(fa); fa = null; }
            if (fb != null && !fb.StartsWith(b.Prefix, StringComparison.Ordinal)) { Add(fb); fb = null; }
            if (fa == null && fb == null) return Remaining > 0 ? null : b; // no keys under it (or no budget to tell)
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
        if (cuts.Count > 0)
        {
            async Task<bool?> SharedAsync(int length)
            {
                string p = common.Substring(0, length);
                foreach (var side in new[] { (Side: _a, On: onA), (Side: _b, On: onB) })
                {
                    if (!side.On) continue;
                    if (!Take()) return null;
                    var after = await ProbeOneAsync(side.Side, p + MaxCodePoint, ct).ConfigureAwait(false);
                    if (after == null) continue;
                    Add(after);
                    if (after.StartsWith(b.Prefix, StringComparison.Ordinal)) return false;
                }
                return true;
            }

            if (await SharedAsync(cuts[0]).ConfigureAwait(false) == true)
            {
                int lo = 0, hi = cuts.Count - 1; // cuts[lo] is shared; find the last shared cut
                while (lo < hi)
                {
                    int mid = (lo + hi + 1) / 2;
                    var shared = await SharedAsync(cuts[mid]).ConfigureAwait(false);
                    if (shared == null) break;
                    if (shared.Value) lo = mid; else hi = mid - 1;
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
            var key = await ProbeOneAsync(p.Side, p.Probe, token).ConfigureAwait(false);
            if (key != null) lock (gate) found.Add((p.Base, key));
        }).ConfigureAwait(false);
        return found;
    }

    /// <summary>The first relative key after prefix + probe, or null (none, or out of budget).</summary>
    private async Task<string?> ProbeOneAsync(Side side, string probe, CancellationToken ct)
    {
        if (!TryTakeRequest()) return null;
        await BeforeRequestAsync(side, ct).ConfigureAwait(false);
        var page = await side.Lister.ListPageAsync(side.Bucket, side.Prefix, side.Prefix + probe, null, null, 1, ct).ConfigureAwait(false);
        var key = page.Objects.FirstOrDefault()?.Key;
        if (key == null || !key.StartsWith(side.Prefix, StringComparison.Ordinal)) return null;
        return key.Substring(side.Prefix.Length);
    }

    /// <summary>Honours the scan's pause and the bucket's request rate before every planner request.</summary>
    private async Task BeforeRequestAsync(Side side, CancellationToken ct)
    {
        if (_pauseGate != null) await _pauseGate(ct).ConfigureAwait(false);
        if (side.Limiter != null) await side.Limiter.WaitAsync(ct).ConfigureAwait(false);
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
