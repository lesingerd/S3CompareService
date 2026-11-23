using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;

namespace S3CompareService.Tests;

/// <summary>The range splitter (work stealing) and the planner's descent into big folders.</summary>
[TestClass]
public class RangeSplitterTests
{
    private static S3ObjectSummary Obj(string key) => new() { Key = key, Size = 1, ETag = "e" };

    private static RangeSplitter Splitter(IEnumerable<string> keys, string prefix = "") =>
        new(new RangeSplitter.Side(new FakeS3Lister(keys.Select(k => Obj(prefix + k))), "a", prefix, null),
            new RangeSplitter.Side(new FakeS3Lister(keys.Select(k => Obj(prefix + k))), "b", prefix, null));

    /// <summary>A few near-empty top folders and one with hundreds of subfolders (the shape that left one range with most keys).</summary>
    internal static List<string> Lopsided()
    {
        var keys = new List<string>();
        foreach (var r in new[] { "a", "b", "c", "e", "f", "g", "h" }) keys.Add($"{r}/CV/_DIRECTORY_HOLDER_");
        for (int v = 0; v < 300; v++) for (int c = 0; c < 3; c++) keys.Add($"d/CV/V_{v:000}/CHUNK_{c}/data");
        return keys;
    }

    [TestMethod]
    public async Task FolderWalk_PicksTheMiddleSubfolderAhead()
    {
        var s = Splitter(Lopsided(), "pre/");
        var key = await s.FindSplitAsync("d/CV/V_010/CHUNK_1/data", "e/CV/", CancellationToken.None);
        Assert.AreEqual("folder", s.Method);
        Assert.IsNotNull(key);
        StringAssert.StartsWith(key, "d/CV/V_");
        int v = int.Parse(key.Substring("d/CV/V_".Length, 3));
        Assert.IsTrue(v >= 140 && v <= 170, key);
    }

    [TestMethod]
    public async Task FolderWalk_CutsInsideTheBigFolder_EvenWithSmallTopFoldersAhead()
    {
        var s = Splitter(Lopsided());
        var key = await s.FindSplitAsync("d/CV/V_010/CHUNK_1/data", null, CancellationToken.None);
        Assert.IsNotNull(key);
        StringAssert.StartsWith(key, "d/CV/V_", "the few small top folders after the big one must not pull the cut out of it");
    }

    [TestMethod]
    public async Task FolderWalk_StaysInsideTheUpperBound()
    {
        var s = Splitter(Lopsided());
        var key = await s.FindSplitAsync("d/CV/V_010/CHUNK_1/data", "d/CV/V_050/", CancellationToken.None);
        Assert.IsNotNull(key);
        Assert.IsTrue(Utf8OrdinalComparer.CompareStatic(key, "d/CV/V_010/CHUNK_1/data") > 0, key);
        Assert.IsTrue(Utf8OrdinalComparer.CompareStatic(key, "d/CV/V_050/") < 0, key);
    }

    [TestMethod]
    public async Task FlatNamespace_UsesProbes()
    {
        var keys = Enumerable.Range(0, 5000).Select(i => $"file_{i:00000}.dat").ToList();
        var s = Splitter(keys);
        var key = await s.FindSplitAsync("file_00100.dat", null, CancellationToken.None);
        Assert.AreEqual("probe", s.Method);
        Assert.IsNotNull(key);
        Assert.IsTrue(Utf8OrdinalComparer.CompareStatic(key, "file_00100.dat") > 0, key);

        var bounded = await s.FindSplitAsync("file_00100.dat", "file_03000.dat", CancellationToken.None);
        Assert.IsNotNull(bounded);
        Assert.IsTrue(Utf8OrdinalComparer.CompareStatic(bounded, "file_00100.dat") > 0 && Utf8OrdinalComparer.CompareStatic(bounded, "file_03000.dat") < 0, bounded);
    }

    [TestMethod]
    public async Task NothingAhead_ReturnsNull()
    {
        var keys = Enumerable.Range(0, 50).Select(i => $"x/{i:00}").ToList();
        var s = Splitter(keys);
        Assert.IsNull(await s.FindSplitAsync("x/49", null, CancellationToken.None));
    }

    [TestMethod]
    public async Task FailingLister_ReturnsNull_AndCountsTheErrors()
    {
        var bad = new FakeS3Lister(Lopsided().Select(Obj)) { Fault = _ => new InvalidOperationException("SlowDown (simulated)") };
        var s = new RangeSplitter(new RangeSplitter.Side(bad, "a", "", null), new RangeSplitter.Side(bad, "b", "", null), TimeSpan.FromSeconds(2));
        Assert.IsNull(await s.FindSplitAsync("d/CV/V_010/CHUNK_1/data", null, CancellationToken.None));
        Assert.IsTrue(s.RequestErrors > 0);
        Assert.IsNotNull(s.LastError);
    }

    [TestMethod]
    public async Task Planner_DescendsIntoTheBigFolder_WhenTheTopLevelIsLopsided()
    {
        var keys = Lopsided();
        var planner = new PartitionPlanner(new FakeS3Lister(keys.Select(Obj)), "a", "", null, new FakeS3Lister(keys.Select(Obj)), "b", "", null, 16, 3, 500);
        var ranges = await planner.PlanAsync(CancellationToken.None);
        Assert.AreEqual(16, ranges.Count);
        int inBig = ranges.Count(r => r.Upper != null && r.Upper.StartsWith("d/CV/V_", StringComparison.Ordinal));
        Assert.IsTrue(inBig >= 12, $"{inBig} of 15 boundaries inside the big folder: {string.Join(" ", ranges)}");
        Assert.IsNull(PartitionPlanner.ValidateTiling(ranges));
    }
}
