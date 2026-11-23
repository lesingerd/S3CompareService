namespace S3CompareService.Core.Engine;

/// <summary>
/// Finds a key that cuts the unfinished rest of a running key range in two, so an idle worker can take the second half
/// (work stealing). Keys are RELATIVE to each bucket's prefix, as in <see cref="PartitionPlanner"/>.
///
/// <para>Folder walk (the usual case): for every "/" folder that contains the worker's position (the root, then each deeper
/// folder), a delimiter listing with StartAfter = position shows the subfolders still ahead of the worker, on both buckets at
/// once. The subfolders ahead of all those levels together (and before the range's upper bound), in key order, are the rest
/// of the current folder at every depth followed by its later siblings; the level with the most is paged on (up to <see cref="MaxPagesPerLevel"/> pages) and the middle one becomes the split key. The worker keeps
/// the first half; everything after the split key (including any subfolders not paged through) goes to the new range.</para>
///
/// <para>Flat fallback: when fewer than two subfolders are ahead, a small probe-only <see cref="PartitionPlanner"/> runs over
/// the worker's deepest folder and the middle candidate between the position and the upper bound is used.</para>
///
/// The split is only a suggestion: the engine applies it under its state lock and discards it if the worker has already
/// moved past it. Every request has its own timeout; a failed request just means no split this time.
/// </summary>
public sealed class RangeSplitter
{
    /// <summary>Pages of one folder listing read while looking for the middle subfolder.</summary>
    public const int MaxPagesPerLevel = 5;

    /// <summary>Requests the flat (probe) fallback may spend per attempt.</summary>
    public const int ProbeBudget = 40;

    /// <summary>Most folder levels listed per attempt (the shallowest ones).</summary>
    public const int MaxLevels = 8;

    public sealed record Side(IS3Lister Lister, string Bucket, string Prefix, IRequestRateLimiter? Limiter);

    private readonly Side _a;
    private readonly Side _b;
    private readonly TimeSpan _requestTimeout;
    private int _requests;
    private int _errors;

    public RangeSplitter(Side a, Side b, TimeSpan? requestTimeout = null)
    {
        _a = a ?? throw new ArgumentNullException(nameof(a));
        _b = b ?? throw new ArgumentNullException(nameof(b));
        _requestTimeout = requestTimeout is { } t && t > TimeSpan.Zero ? t : PartitionPlanner.DefaultRequestTimeout;
    }

    /// <summary>S3 requests sent by the last <see cref="FindSplitAsync"/>.</summary>
    public int RequestsUsed => Volatile.Read(ref _requests);

    /// <summary>Requests of the last <see cref="FindSplitAsync"/> that failed or timed out (skipped; never fatal).</summary>
    public int RequestErrors => Volatile.Read(ref _errors);

    /// <summary>The most recent request error, or null.</summary>
    public string? LastError { get; private set; }

    /// <summary>How the last split key was found: "folder", "probe" or "none".</summary>
    public string Method { get; private set; } = "none";

    /// <summary>
    /// A relative key S with <paramref name="position"/> &lt; S and (<paramref name="upper"/> null or S &lt; upper), splitting the
    /// rest of the range roughly in half, or null when none was found. Throws only when <paramref name="ct"/> is cancelled.
    /// </summary>
    /// <param name="position">The worker's current key (relative); null or empty = the range has not started.</param>
    /// <param name="upper">The range's inclusive upper bound (relative); null = the end of the keyspace.</param>
    public async Task<string?> FindSplitAsync(string? position, string? upper, CancellationToken ct)
    {
        _requests = 0;
        _errors = 0;
        LastError = null;
        Method = "none";
        string pos = position ?? string.Empty;

        // Folders containing the position: "", "a/", "a/b/", ...
        var levels = new List<string> { string.Empty };
        for (int i = pos.IndexOf('/'); i >= 0 && levels.Count < MaxLevels; i = pos.IndexOf('/', i + 1))
        {
            levels.Add(pos.Substring(0, i + 1));
        }

        // First page of every level, concurrently. The folders ahead of all levels together, in key order, are the rest of the
        // current folder at every depth followed by its later siblings: the middle one by count cuts where folders are dense,
        // which is where a lopsided bucket keeps its objects (a few top folders after the big one do not pull the cut out of it).
        var firstPages = await Task.WhenAll(levels.Select(l => FolderPageAsync(l, pos, null, null, upper, ct))).ConfigureAwait(false);
        var folders = new SortedSet<string>(Utf8OrdinalComparer.Instance);
        foreach (var page in firstPages) folders.UnionWith(page.Folders);

        // Page on through the level with the most folders while it is truncated and still inside the range.
        int big = Enumerable.Range(0, levels.Count).OrderByDescending(i => firstPages[i].Folders.Count).First();
        var more = firstPages[big];
        for (int p = 1; p < MaxPagesPerLevel && (more.TokenA != null || more.TokenB != null) && !more.PastUpper && more.Folders.Count > 0; p++)
        {
            more = await FolderPageAsync(levels[big], pos, more.TokenA, more.TokenB, upper, ct).ConfigureAwait(false);
            folders.UnionWith(more.Folders);
        }

        if (folders.Count >= 2)
        {
            Method = "folder";
            return folders.ElementAt(folders.Count / 2);
        }

        // Flat (or nearly flat) below the position: probe the deepest folder.
        string? probed = await ProbeAsync(levels[^1], pos, upper, ct).ConfigureAwait(false);
        if (probed != null)
        {
            Method = "probe";
            return probed;
        }

        // Last resort: the single folder ahead.
        if (folders.Count == 1)
        {
            Method = "folder";
            return folders.Min;
        }
        return null;
    }

    private sealed record FolderPage(List<string> Folders, string? TokenA, string? TokenB, bool PastUpper);

    /// <summary>
    /// One delimiter page of <paramref name="folder"/> on both buckets (or the continuation of each), returning the subfolders
    /// strictly between <paramref name="pos"/> and <paramref name="upper"/>. A null token for a side that had one means it is done.
    /// </summary>
    private async Task<FolderPage> FolderPageAsync(string folder, string pos, string? tokenA, string? tokenB, string? upper, CancellationToken ct)
    {
        bool first = tokenA == null && tokenB == null;
        var ta = first || tokenA != null ? ListAsync(_a, folder, pos, tokenA, ct) : Task.FromResult<S3ListPage?>(null);
        var tb = first || tokenB != null ? ListAsync(_b, folder, pos, tokenB, ct) : Task.FromResult<S3ListPage?>(null);
        var pages = await Task.WhenAll(ta, tb).ConfigureAwait(false);

        var folders = new SortedSet<string>(Utf8OrdinalComparer.Instance);
        bool pastUpper = false;
        string? nextA = null, nextB = null;
        for (int s = 0; s < 2; s++)
        {
            var page = pages[s];
            if (page == null) continue;
            var side = s == 0 ? _a : _b;
            foreach (var cp in page.CommonPrefixes)
            {
                if (!cp.StartsWith(side.Prefix, StringComparison.Ordinal)) continue;
                string rel = cp.Substring(side.Prefix.Length);
                if (Utf8OrdinalComparer.CompareStatic(rel, pos) <= 0) continue;
                if (upper != null && Utf8OrdinalComparer.CompareStatic(rel, upper) >= 0) { pastUpper = true; continue; }
                folders.Add(rel);
            }
            // Keys come back in order, so an object past the upper bound means nothing further on this side is inside the range.
            var lastObject = page.Objects.Count > 0 ? page.Objects[^1].Key : null;
            if (upper != null && lastObject != null && lastObject.StartsWith(side.Prefix, StringComparison.Ordinal)
                && Utf8OrdinalComparer.CompareStatic(lastObject.Substring(side.Prefix.Length), upper) >= 0) pastUpper = true;
            string? next = page.IsTruncated && !string.IsNullOrEmpty(page.NextContinuationToken) ? page.NextContinuationToken : null;
            if (s == 0) nextA = next; else nextB = next;
        }
        return new FolderPage(folders.ToList(), nextA, nextB, pastUpper);
    }

    private async Task<S3ListPage?> ListAsync(Side side, string folder, string pos, string? token, CancellationToken ct)
    {
        Interlocked.Increment(ref _requests);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        try
        {
            if (side.Limiter != null) await side.Limiter.WaitAsync(ct).ConfigureAwait(false);
            cts.CancelAfter(_requestTimeout);
            // StartAfter is ignored by S3 when a continuation token is given, and it only applies when it sorts after the prefix.
            string startAfter = side.Prefix + pos;
            return await side.Lister.ListPageAsync(side.Bucket, side.Prefix + folder, token == null && startAfter.Length > 0 ? startAfter : null, token, "/", 1000, cts.Token)
                .WaitUnlessDoneAsync(_requestTimeout, cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ct.ThrowIfCancellationRequested();
            Interlocked.Increment(ref _errors);
            LastError = ex is TimeoutException or OperationCanceledException
                ? $"split listing of '{InstrumentedLister.Clip(side.Prefix + folder)}' timed out after {_requestTimeout.TotalSeconds:0} s"
                : $"split listing of '{InstrumentedLister.Clip(side.Prefix + folder)}': {ex.GetType().Name}: {ex.Message}";
            return null;
        }
    }

    /// <summary>Probe-only plan of <paramref name="folder"/>; the middle candidate strictly between the position and the upper bound.</summary>
    private async Task<string?> ProbeAsync(string folder, string pos, string? upper, CancellationToken ct)
    {
        var planner = new PartitionPlanner(
            _a.Lister, _a.Bucket, _a.Prefix + folder, _a.Limiter,
            _b.Lister, _b.Bucket, _b.Prefix + folder, _b.Limiter,
            targetPartitions: 16, maxDiscoveryDepth: 0, maxRequests: ProbeBudget,
            requestTimeout: _requestTimeout);
        await planner.PlanAsync(ct).ConfigureAwait(false);
        Interlocked.Add(ref _requests, planner.RequestsUsed);
        Interlocked.Add(ref _errors, planner.RequestErrors);
        if (planner.LastError != null) LastError = planner.LastError;

        var inside = planner.Candidates
            .Select(c => folder + c)
            .Where(k => Utf8OrdinalComparer.CompareStatic(k, pos) > 0 && (upper == null || Utf8OrdinalComparer.CompareStatic(k, upper) < 0))
            .ToList();
        return inside.Count == 0 ? null : inside[inside.Count / 2];
    }
}
