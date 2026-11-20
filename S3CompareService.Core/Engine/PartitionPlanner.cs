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
/// <item>Probe fallback for flat or shallow namespaces: for each printable ASCII character, a MaxKeys=1 listing with
/// StartAfter = prefix + probe on both buckets; the keys found are the candidates. If still too few, it probes one character
/// deeper below the prefixes found (up to 4 rounds, sized to the remaining budget).</item>
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
    private const int MaxInitialProbeBases = 32;

    /// <summary>Characters probed when the budget does not allow the whole alphabet (ASCII order).</summary>
    private const string CommonKeyChars = "-./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

    private readonly Side _a;
    private readonly Side _b;
    private readonly int _targetPartitions;
    private readonly int _maxDepth;
    private readonly int _maxRequests;
    private int _requestsUsed;
    private List<string>? _deepestLevel;

    private sealed record Side(IS3Lister Lister, string Bucket, string Prefix, IRequestRateLimiter? Limiter);

    public PartitionPlanner(
        IS3Lister listerA, string bucketA, string? prefixA, IRequestRateLimiter? limiterA,
        IS3Lister listerB, string bucketB, string? prefixB, IRequestRateLimiter? limiterB,
        int targetPartitions,
        int maxDiscoveryDepth = ScanOptions.DefaultMaxDiscoveryDepth,
        int maxRequests = ScanOptions.DefaultMaxDiscoveryRequests)
    {
        _a = new Side(listerA ?? throw new ArgumentNullException(nameof(listerA)), bucketA, prefixA ?? string.Empty, limiterA);
        _b = new Side(listerB ?? throw new ArgumentNullException(nameof(listerB)), bucketB, prefixB ?? string.Empty, limiterB);
        _targetPartitions = Math.Max(1, targetPartitions);
        _maxDepth = Math.Max(0, maxDiscoveryDepth);
        _maxRequests = Math.Max(0, maxRequests);
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
            if (side.Limiter != null) await side.Limiter.WaitAsync(ct).ConfigureAwait(false);
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
    /// Iterative deepening over key prefixes ("bases"). Round 0 probes every character after each base; each later round
    /// probes one character deeper, below the prefixes the previous round found, with as many characters as the remaining
    /// budget allows. So keys that share a long common prefix (zero-padded numbers, one big folder) still get boundaries.
    /// The first bases are the start of the keyspace plus the deepest "/" level found by discovery (when it is small).
    /// </summary>
    private async Task<bool> ProbeAsync(SortedSet<string> candidates, CancellationToken ct)
    {
        bool any = false;
        var bases = new List<string> { string.Empty };
        if (_deepestLevel is { Count: > 0 and <= MaxInitialProbeBases }) bases.AddRange(_deepestLevel);

        for (int round = 0; round < MaxProbeRounds && bases.Count > 0 && candidates.Count < _targetPartitions; round++)
        {
            int remaining = _maxRequests - RequestsUsed;
            if (remaining < 2) break;
            int roundsLeft = MaxProbeRounds - round;
            int maxProbes = remaining / 2;
            if (bases.Count > maxProbes) bases = EvenlySpaced(bases, maxProbes);
            int perBase = round == 0
                ? ProbeAlphabet.Length
                : Math.Clamp(remaining / (2 * bases.Count * roundsLeft), 1, ProbeAlphabet.Length);
            var chars = ProbeChars(perBase);

            var probes = bases.SelectMany(b => chars.Select(c => (Base: b, Probe: b + c))).ToList();
            var found = await RunProbesAsync(probes, ct).ConfigureAwait(false);
            if (found.Count > 0) any = true;
            candidates.UnionWith(found.Select(f => f.Key));

            // Next round: one character deeper below every prefix this round found keys under.
            bases = found
                .Where(f => f.Key.Length > f.Base.Length + 1 && f.Key.StartsWith(f.Base, StringComparison.Ordinal))
                .Select(f => f.Key.Substring(0, f.Base.Length + 1))
                .Distinct(StringComparer.Ordinal)
                .OrderBy(b => b, Utf8OrdinalComparer.Instance)
                .ToList();
        }
        return any;
    }

    /// <summary>
    /// <paramref name="count"/> probe characters: the whole alphabet, or the common key characters (digits, letters, - . / _)
    /// evenly spaced when the budget allows fewer.
    /// </summary>
    private static List<char> ProbeChars(int count)
    {
        if (count >= ProbeAlphabet.Length) return ProbeAlphabet.ToList();
        return EvenlySpaced(CommonKeyChars.ToList(), Math.Min(count, CommonKeyChars.Length));
    }

    private static List<T> EvenlySpaced<T>(List<T> items, int count)
    {
        if (count >= items.Count) return items;
        var list = new List<T>(count);
        for (int i = 0; i < count; i++) list.Add(items[(int)((long)i * items.Count / count)]);
        return list;
    }

    private async Task<List<(string Base, string Key)>> RunProbesAsync(List<(string Base, string Probe)> probes, CancellationToken ct)
    {
        var found = new List<(string Base, string Key)>();
        var gate = new object();
        await Parallel.ForEachAsync(probes, new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentPlannerRequests, CancellationToken = ct }, async (p, token) =>
        {
            var keys = await Task.WhenAll(ProbeOneAsync(_a, p.Probe, token), ProbeOneAsync(_b, p.Probe, token)).ConfigureAwait(false);
            lock (gate)
            {
                foreach (var k in keys) if (k != null) found.Add((p.Base, k));
            }
        }).ConfigureAwait(false);
        return found;
    }

    /// <summary>The first relative key after prefix + probe, or null (none, or out of budget).</summary>
    private async Task<string?> ProbeOneAsync(Side side, string probe, CancellationToken ct)
    {
        if (!TryTakeRequest()) return null;
        if (side.Limiter != null) await side.Limiter.WaitAsync(ct).ConfigureAwait(false);
        var page = await side.Lister.ListPageAsync(side.Bucket, side.Prefix, side.Prefix + probe, null, null, 1, ct).ConfigureAwait(false);
        var key = page.Objects.FirstOrDefault()?.Key;
        if (key == null || !key.StartsWith(side.Prefix, StringComparison.Ordinal)) return null;
        return key.Substring(side.Prefix.Length);
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
