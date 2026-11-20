using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;

namespace S3CompareService.Tests;

[TestClass]
public class PrefetchStreamTests
{
    private static S3ObjectSummary Obj(string key) => new() { Key = key, Size = 1, ETag = "e", LastModified = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), StorageClass = "STANDARD" };

    private static async Task<List<string>> DrainAsync(IS3ObjectStream s, CancellationToken ct = default)
    {
        var keys = new List<string>();
        while (await s.MoveNextAsync(ct)) keys.Add(s.Current!.Key);
        return keys;
    }

    [TestMethod]
    public async Task Prefetch_ReturnsKeysInOrder_HonouringPrefixStartAfterAndUpperBound()
    {
        var items = Enumerable.Range(0, 200).Select(i => Obj($"p/k{i:000}")).Append(Obj("other/x")).Append(Obj("p")).ToList();
        var lister = new FakeS3Lister(items);

        await using var all = new PrefetchingObjectStream(lister, "b", "p/", null, null, 2, null, pageSize: 7);
        var keys = await DrainAsync(all);
        CollectionAssert.AreEqual(Enumerable.Range(0, 200).Select(i => $"p/k{i:000}").ToList(), keys);
        Assert.AreEqual(200, all.TotalObjectsRead);
        Assert.AreEqual(200, all.StorageClassCounts["STANDARD"]);

        await using var range = new PrefetchingObjectStream(lister, "b", "p/", "p/k010", "k150", 2, null, pageSize: 7);
        CollectionAssert.AreEqual(Enumerable.Range(11, 140).Select(i => $"p/k{i:000}").ToList(), await DrainAsync(range));
        // It stops at the upper bound instead of listing to the end: about (150 - 10) / 7 pages, not 190 / 7.
        Assert.IsTrue(range.PagesRequested <= 22, $"pages: {range.PagesRequested}");
    }

    [TestMethod]
    public async Task Prefetch_ProducerException_SurfacesOnMoveNext_AfterEarlierPages()
    {
        var lister = new FakeS3Lister(Enumerable.Range(0, 100).Select(i => Obj($"k{i:000}")))
        {
            Fault = call => call == 3 ? new InvalidOperationException("SlowDown (simulated)") : null
        };
        await using var s = new PrefetchingObjectStream(lister, "b", null, null, null, 2, null, pageSize: 10);
        int read = 0;
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            while (await s.MoveNextAsync()) read++;
        });
        StringAssert.Contains(ex.Message, "SlowDown");
        Assert.AreEqual(20, read, "both pages that arrived before the failure were delivered");
        await Assert.ThrowsAsync<InvalidOperationException>(() => s.MoveNextAsync(), "the error stays visible");
    }

    [TestMethod]
    public async Task Prefetch_IsBounded_ByPrefetchPages()
    {
        var lister = new FakeS3Lister(Enumerable.Range(0, 1000).Select(i => Obj($"k{i:0000}")));
        await using var s = new PrefetchingObjectStream(lister, "b", null, null, null, 2, null, pageSize: 10);
        await Task.Delay(300);
        // Two pages buffered plus one fetched and waiting to be written; never the whole listing.
        Assert.IsTrue(s.PagesRequested is >= 2 and <= 3, $"requested {s.PagesRequested}");
        Assert.AreEqual(1000, (await DrainAsync(s)).Count);
        Assert.AreEqual(100, s.PagesRequested);
    }

    [TestMethod]
    public async Task Prefetch_DisposeAndCancellation_DoNotHang()
    {
        var slow = new FakeS3Lister(new[] { Obj("a") }, latency: TimeSpan.FromSeconds(30));

        var s1 = new PrefetchingObjectStream(slow, "b", null, null, null, 2, null);
        var dispose = s1.DisposeAsync().AsTask();
        Assert.AreSame(dispose, await Task.WhenAny(dispose, Task.Delay(5000)), "DisposeAsync cancels and awaits the in-flight request");

        await using var s2 = new PrefetchingObjectStream(slow, "b", null, null, null, 2, null);
        using var cts = new CancellationTokenSource(100);
        var move = s2.MoveNextAsync(cts.Token);
        Assert.AreSame(move, await Task.WhenAny(move, Task.Delay(5000)));
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await move);

        // Cancelling the token the stream was created with stops the producer; the consumer then sees the cancellation.
        using var scanCts = new CancellationTokenSource();
        await using var s3 = new PrefetchingObjectStream(slow, "b", null, null, null, 2, null, cancellationToken: scanCts.Token);
        scanCts.CancelAfter(100);
        var move3 = s3.MoveNextAsync();
        Assert.AreSame(move3, await Task.WhenAny(move3, Task.Delay(5000)));
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await move3);
    }

    private sealed class TruncatedWithoutTokenLister : IS3Lister
    {
        public Task<S3ListPage> ListPageAsync(string bucket, string? prefix, string? startAfter, string? continuationToken, string? delimiter, int maxKeys, CancellationToken cancellationToken)
            => Task.FromResult(new S3ListPage { Objects = new() { Obj("a") }, IsTruncated = true, NextContinuationToken = null });
    }

    [TestMethod]
    public async Task Prefetch_TruncatedWithoutToken_ThrowsInsteadOfLooping()
    {
        await using var s = new PrefetchingObjectStream(new TruncatedWithoutTokenLister(), "b", null, null, null, 2, null);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => DrainAsync(s));
        StringAssert.Contains(ex.Message, "NextContinuationToken");
    }

    [TestMethod]
    public async Task RangeStreams_OverRandomBoundaries_ReturnEveryKeyExactlyOnce()
    {
        var rng = new Random(7);
        string[] alphabet = { "a", "b", "z", "0", "9", "/", "-", "ü", "日", "！", "\U0001F600", "~" };
        string RandomKey() => string.Concat(Enumerable.Range(0, rng.Next(1, 6)).Select(_ => alphabet[rng.Next(alphabet.Length)]));
        var keys = Enumerable.Range(0, 600).Select(_ => RandomKey()).Distinct().ToList();
        var lister = new FakeS3Lister(keys.Select(k => Obj("pre/" + k)));
        var expected = keys.OrderBy(k => k, Utf8OrdinalComparer.Instance).Select(k => "pre/" + k).ToList();

        for (int round = 0; round < 15; round++)
        {
            // Boundaries: some real keys (so keys sit exactly on boundaries) and some strings that are not keys.
            var candidates = Enumerable.Range(0, rng.Next(0, 30)).Select(_ => rng.Next(2) == 0 ? keys[rng.Next(keys.Count)] : RandomKey());
            var ranges = PartitionPlanner.BuildRanges(candidates, rng.Next(1, 25));
            Assert.IsNull(PartitionPlanner.ValidateTiling(ranges));

            var got = new List<string>();
            foreach (var r in ranges)
            {
                await using var s = new PrefetchingObjectStream(lister, "b", "pre/", ServiceComparisonEngine.BuildStartAfterKey(r.Lower != null, r.Lower, "pre/"), r.Upper, 2, null, pageSize: 9);
                got.AddRange(await DrainAsync(s));
            }
            CollectionAssert.AreEqual(expected, got, $"round {round}: ranges {string.Join(" ", ranges)}");
        }
    }
}

[TestClass]
public class PartitionPlannerTests
{
    private static S3ObjectSummary Obj(string key) => new() { Key = key, Size = 1, ETag = "e" };

    private static PartitionPlanner Planner(IEnumerable<string> keysA, IEnumerable<string> keysB, int target, string prefixA = "", string prefixB = "", int maxRequests = 500, int depth = 3)
        => new(new FakeS3Lister(keysA.Select(Obj)), "a", prefixA, null, new FakeS3Lister(keysB.Select(Obj)), "b", prefixB, null, target, depth, maxRequests);

    private static void AssertTiles(List<PartitionRange> ranges, IEnumerable<string>? relativeKeys = null)
    {
        Assert.IsNull(PartitionPlanner.ValidateTiling(ranges), string.Join(" ", ranges));
        foreach (var key in relativeKeys ?? Enumerable.Empty<string>())
        {
            int containing = ranges.Count(r =>
                (r.Lower == null || Utf8OrdinalComparer.CompareStatic(key, r.Lower) > 0) &&
                (r.Upper == null || Utf8OrdinalComparer.CompareStatic(key, r.Upper) <= 0));
            Assert.AreEqual(1, containing, $"key '{key}' must be in exactly one range");
            int idx = PartitionPlanner.IndexOf(ranges, key);
            var r = ranges[idx];
            Assert.IsTrue((r.Lower == null || Utf8OrdinalComparer.CompareStatic(key, r.Lower) > 0) && (r.Upper == null || Utf8OrdinalComparer.CompareStatic(key, r.Upper) <= 0));
        }
    }

    [TestMethod]
    public async Task Hierarchical_UsesDelimiterDiscovery_WithFewRequests()
    {
        var keys = Enumerable.Range(0, 40).SelectMany(d => Enumerable.Range(0, 10).Select(f => $"d{d:00}/f{f:00}")).ToList();
        var planner = Planner(keys, keys, target: 8);
        var ranges = await planner.PlanAsync(CancellationToken.None);

        Assert.AreEqual("delimiter", planner.Method);
        Assert.AreEqual(8, ranges.Count);
        Assert.AreEqual(2, planner.RequestsUsed, "one '/' listing per bucket");
        Assert.IsTrue(ranges.Take(7).All(r => r.Upper!.StartsWith('d') && r.Upper.EndsWith('/')));
        AssertTiles(ranges, keys);
    }

    [TestMethod]
    public async Task Hierarchical_DescendsLevels_UntilEnoughBoundaries()
    {
        var keys = new[] { "a/", "b/" }.SelectMany(top => Enumerable.Range(0, 12).SelectMany(s => Enumerable.Range(0, 3).Select(f => $"{top}s{s:00}/f{f}"))).ToList();
        var planner = Planner(keys, keys, target: 16);
        var ranges = await planner.PlanAsync(CancellationToken.None);

        Assert.AreEqual("delimiter", planner.Method);
        Assert.AreEqual(16, ranges.Count);
        Assert.IsTrue(ranges.Any(r => r.Upper != null && r.Upper.Count(c => c == '/') == 2), "second level boundaries were used");
        AssertTiles(ranges, keys);
    }

    [TestMethod]
    public async Task Flat_FallsBackToProbing()
    {
        var rng = new Random(3);
        var keys = Enumerable.Range(0, 3000).Select(_ => rng.Next().ToString("x8")).Distinct().ToList();
        var planner = Planner(keys, keys.Skip(100), target: 12);
        var ranges = await planner.PlanAsync(CancellationToken.None);

        Assert.AreEqual("probe", planner.Method);
        Assert.AreEqual(12, ranges.Count);
        Assert.IsTrue(planner.RequestsUsed <= 500);
        AssertTiles(ranges, keys);

        // Ranges are reasonably even: no range holds more than half the keys.
        int maxInRange = keys.GroupBy(k => PartitionPlanner.IndexOf(ranges, k)).Max(g => g.Count());
        Assert.IsTrue(maxInRange < keys.Count / 2, $"largest range has {maxInRange} keys");
    }

    [TestMethod]
    public async Task Flat_WithLongCommonPrefix_ProbesDeeper()
    {
        var keys = Enumerable.Range(0, 5000).Select(i => $"{i:000000}.log").ToList();
        var planner = Planner(keys, keys, target: 8);
        var ranges = await planner.PlanAsync(CancellationToken.None);

        Assert.IsTrue(ranges.Count >= 4, $"zero-padded names still get boundaries: {string.Join(" ", ranges)}");
        Assert.IsTrue(planner.RequestsUsed <= 500);
        AssertTiles(ranges, keys);
    }

    [TestMethod]
    public async Task EmptyBuckets_GiveOneRange()
    {
        var planner = Planner(Array.Empty<string>(), Array.Empty<string>(), target: 8);
        var ranges = await planner.PlanAsync(CancellationToken.None);
        Assert.AreEqual(1, ranges.Count);
        Assert.IsNull(ranges[0].Lower);
        Assert.IsNull(ranges[0].Upper);
    }

    [TestMethod]
    public async Task RequestBudget_IsRespected()
    {
        var rng = new Random(5);
        var keys = Enumerable.Range(0, 2000).Select(_ => rng.Next().ToString("x8")).ToList();
        var planner = Planner(keys, keys, target: 64, maxRequests: 10);
        var ranges = await planner.PlanAsync(CancellationToken.None);
        Assert.IsTrue(planner.RequestsUsed <= 10);
        AssertTiles(ranges, keys);

        var none = Planner(keys, keys, target: 64, maxRequests: 0);
        Assert.AreEqual(1, (await none.PlanAsync(CancellationToken.None)).Count, "budget 0 = no planning");
    }

    [TestMethod]
    public async Task DifferentPrefixes_GiveRelativeBoundaries()
    {
        var rel = Enumerable.Range(0, 20).SelectMany(d => Enumerable.Range(0, 5).Select(f => $"dir{d:00}/f{f}")).ToList();
        var keysA = rel.Select(k => "src/" + k).Append("zzz/outside").ToList();
        var keysB = rel.Where((_, i) => i % 3 != 0).Select(k => "backup/2024/" + k).Append("aaa/outside").ToList();
        var planner = Planner(keysA, keysB, target: 6, prefixA: "src/", prefixB: "backup/2024/");
        var ranges = await planner.PlanAsync(CancellationToken.None);

        Assert.AreEqual(6, ranges.Count);
        Assert.IsTrue(ranges.Where(r => r.Upper != null).All(r => r.Upper!.StartsWith("dir", StringComparison.Ordinal)), string.Join(" ", ranges));
        AssertTiles(ranges, rel);
    }

    [TestMethod]
    public async Task UnicodeKeys_AreOrderedAsUtf8()
    {
        var dirs = new[] { "a", "z", "ü", "日本", "！", "\U0001F600", "Ω" };
        var keys = dirs.SelectMany(d => Enumerable.Range(0, 4).Select(f => $"{d}/{f}")).ToList();
        var planner = Planner(keys, keys, target: 5);
        var ranges = await planner.PlanAsync(CancellationToken.None);
        AssertTiles(ranges, keys);
        // U+FF01 (fullwidth !) sorts before U+1F600 in UTF-8 (not in UTF-16).
        var uppers = ranges.Where(r => r.Upper != null).Select(r => r.Upper!).ToList();
        CollectionAssert.AreEqual(uppers.OrderBy(u => u, Utf8OrdinalComparer.Instance).ToList(), uppers);
    }

    [TestMethod]
    public void BuildRanges_TilesExactly_ForAnyCandidates()
    {
        var rng = new Random(11);
        string[] alphabet = { "", "a", "b", "/", "0", "ü", "\U0001F600", "！", " " };
        for (int round = 0; round < 200; round++)
        {
            var candidates = Enumerable.Range(0, rng.Next(0, 40)).Select(_ => string.Concat(Enumerable.Range(0, rng.Next(0, 4)).Select(_ => alphabet[rng.Next(alphabet.Length)]))).ToList();
            int target = rng.Next(1, 20);
            var ranges = PartitionPlanner.BuildRanges(candidates, target);
            Assert.IsTrue(ranges.Count >= 1 && ranges.Count <= target);
            var probes = candidates.Concat(Enumerable.Range(0, 20).Select(_ => string.Concat(Enumerable.Range(0, rng.Next(0, 5)).Select(_ => alphabet[rng.Next(alphabet.Length)]))));
            AssertTiles(ranges, probes);
        }
    }

    [TestMethod]
    public void ValidateTiling_RejectsGapsOverlapsAndOpenEnds()
    {
        Assert.IsNotNull(PartitionPlanner.ValidateTiling(new List<PartitionRange>()));
        Assert.IsNotNull(PartitionPlanner.ValidateTiling(new List<PartitionRange> { new() { Lower = "a" } }), "must start at the beginning");
        Assert.IsNotNull(PartitionPlanner.ValidateTiling(new List<PartitionRange> { new() { Upper = "m" } }), "must end at the end");
        Assert.IsNotNull(PartitionPlanner.ValidateTiling(new List<PartitionRange> { new() { Upper = "m" }, new() { Lower = "n" } }), "gap");
        Assert.IsNotNull(PartitionPlanner.ValidateTiling(new List<PartitionRange> { new() { Upper = "m" }, new() { Lower = "m", Upper = "c" }, new() { Lower = "c" } }), "reversed");
        Assert.IsNotNull(PartitionPlanner.ValidateTiling(new List<PartitionRange> { new() { Upper = "m", LastCompletedKey = "x" }, new() { Lower = "m" } }), "progress outside its range");
        Assert.IsNull(PartitionPlanner.ValidateTiling(new List<PartitionRange> { new() { Upper = "m", LastCompletedKey = "m" }, new() { Lower = "m", LastCompletedKey = "n" } }));
    }

    [TestMethod]
    public void Frontier_IsTheLowestUnfinishedPoint()
    {
        var plan = new List<PartitionRange>
        {
            new() { Upper = "g", Completed = true, LastCompletedKey = "f" },
            new() { Lower = "g", Upper = "p", LastCompletedKey = "k" },
            new() { Lower = "p", LastCompletedKey = "x" }
        };
        Assert.AreEqual((true, "k"), PartitionPlanner.Frontier(plan));
        plan[1].Completed = true;
        Assert.AreEqual((true, "x"), PartitionPlanner.Frontier(plan));
        plan[2].Completed = true;
        Assert.AreEqual((true, "x"), PartitionPlanner.Frontier(plan), "all complete: the highest key");
        Assert.AreEqual((false, ""), PartitionPlanner.Frontier(new List<PartitionRange> { new() { Upper = "g" }, new() { Lower = "g", LastCompletedKey = "h" } }));
    }
}
