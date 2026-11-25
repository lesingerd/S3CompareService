using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

/// <summary>
/// The deduplicated difference store: a difference that repeats unchanged in the next scan of the same job is stored once,
/// every run still sees exactly its own differences, and deleting any run (first, middle, latest, all) never changes what the
/// other runs show.
/// </summary>
[TestClass]
public class DeduplicationTests : TestDbBase
{
    private RetentionManager _retention = null!;
    private static readonly RetentionOptions Opts = new() { BatchPause = TimeSpan.Zero, BatchSize = 3 };

    [TestInitialize]
    public void DedupInit() => _retention = new RetentionManager(Factory);

    private static RecordedDifference D(string runId, string key, long size = 10, DifferenceType type = DifferenceType.MissingInBucketB, bool inFlight = false) => new()
    {
        ScanRunId = runId, Key = key, Type = type, Details = $"missing {key}", BucketASize = size, BucketAETag = "\"e\"",
        BucketALastModified = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), IsWithinGracePeriod = inFlight
    };

    /// <summary>Creates a run of the job and stores the given differences as its scan would.</summary>
    private async Task<ScanRun> ScanAsync(string jobId, params (string Key, long Size)[] diffs)
    {
        var run = new ScanRun { JobId = jobId, JobName = jobId, Status = RunStatus.Completed, EndTimeUtc = DateTime.UtcNow };
        await RunRepo.CreateAsync(run);
        await DiffRepo.InsertBatchAsync(diffs.Select(d => D(run.Id, d.Key, d.Size)).ToList());
        return run;
    }

    private async Task<string> ViewAsync(ScanRun run)
    {
        var items = await DiffRepo.GetDifferencesAsync(run.Id, take: 10_000);
        Assert.IsTrue(items.All(d => d.ScanRunId == run.Id), "rows are reported as the run's own");
        Assert.AreEqual(items.Count, await DiffRepo.GetDifferenceCountAsync(run.Id));
        return string.Join(",", items.Select(d => $"{d.Key}:{d.BucketASize}"));
    }

    private Task<long> RecordsAsync() => RawScalarAsync<long>("SELECT COUNT(*) FROM DifferenceRecords");

    private async Task DeleteAsync(ScanRun run)
    {
        Assert.IsNull(await _retention.DeleteRunAsync(run.Id));
        await _retention.DeleteOrphanedRowsAsync(Opts);
    }

    /// <summary>Run 1: a b c. Run 2: a b d (c fixed, d new). Run 3: a b(size changed) d.</summary>
    private async Task<(ScanRun R1, ScanRun R2, ScanRun R3)> SeriesAsync(string job = "j")
    {
        var r1 = await ScanAsync(job, ("a", 1), ("b", 2), ("c", 3));
        var r2 = await ScanAsync(job, ("a", 1), ("b", 2), ("d", 4));
        var r3 = await ScanAsync(job, ("a", 1), ("b", 5), ("d", 4));
        return (r1, r2, r3);
    }

    [TestMethod]
    public async Task RepeatedDifferences_AreStoredOnce_AndEveryRunSeesItsOwn()
    {
        var (r1, r2, r3) = await SeriesAsync();
        Assert.AreEqual("a:1,b:2,c:3", await ViewAsync(r1));
        Assert.AreEqual("a:1,b:2,d:4", await ViewAsync(r2));
        Assert.AreEqual("a:1,b:5,d:4", await ViewAsync(r3));
        Assert.AreEqual(5, await RecordsAsync(), "a, b, c, d and the changed b: 5 records instead of 9 rows");
        var stats = (await DiffRepo.GetStorageStatsAsync(r3.Id))!;
        Assert.AreEqual((true, 3L, 1L, 2L), (stats.Deduplicated, stats.InRun, stats.NewRecords, stats.CarriedOver), "run 3: the changed b is new, a and d carried over");
        Assert.AreEqual(0, await RawScalarAsync<long>("SELECT COUNT(*) FROM RecordedDifferences"));
    }

    [TestMethod]
    public async Task ADifferenceThatDisappearsAndComesBack_IsNotShownInTheRunWithoutIt()
    {
        var r1 = await ScanAsync("j", ("a", 1));
        var r2 = await ScanAsync("j", ("z", 9));
        var r3 = await ScanAsync("j", ("a", 1));
        Assert.AreEqual("a:1", await ViewAsync(r1));
        Assert.AreEqual("z:9", await ViewAsync(r2));
        Assert.AreEqual("a:1", await ViewAsync(r3));
    }

    [TestMethod]
    public async Task OtherJobs_NeverShareRecords()
    {
        var a = await ScanAsync("j1", ("a", 1));
        var b = await ScanAsync("j2", ("a", 1));
        Assert.AreEqual(2, await RecordsAsync());
        await DeleteAsync(a);
        Assert.AreEqual("a:1", await ViewAsync(b));
        Assert.AreEqual(1, await RecordsAsync());
    }

    [TestMethod]
    public async Task DeletingTheFirstRun_KeepsTheOthers_AndFreesWhatOnlyItHad()
    {
        var (r1, r2, r3) = await SeriesAsync();
        await DeleteAsync(r1);
        Assert.AreEqual("a:1,b:2,d:4", await ViewAsync(r2));
        Assert.AreEqual("a:1,b:5,d:4", await ViewAsync(r3));
        Assert.AreEqual(4, await RecordsAsync(), "c existed only in the deleted run");
    }

    [TestMethod]
    public async Task DeletingAMiddleRun_KeepsTheOthers_AndTheNextScanStillDeduplicates()
    {
        var (r1, r2, r3) = await SeriesAsync();
        await DeleteAsync(r2);
        Assert.AreEqual("a:1,b:2,c:3", await ViewAsync(r1));
        Assert.AreEqual("a:1,b:5,d:4", await ViewAsync(r3));
        Assert.AreEqual(5, await RecordsAsync(), "every record is still used by run 1 or run 3");

        var r4 = await ScanAsync("j", ("a", 1), ("b", 5), ("d", 4));
        Assert.AreEqual("a:1,b:5,d:4", await ViewAsync(r4));
        Assert.AreEqual(5, await RecordsAsync(), "nothing new stored");
    }

    [TestMethod]
    public async Task DeletingTheLatestRun_KeepsTheOthers_AndTheNextScanReusesTheOlderRecords()
    {
        var (r1, r2, r3) = await SeriesAsync();
        await DeleteAsync(r3);
        Assert.AreEqual("a:1,b:2,c:3", await ViewAsync(r1));
        Assert.AreEqual("a:1,b:2,d:4", await ViewAsync(r2));
        Assert.AreEqual(4, await RecordsAsync(), "the changed b existed only in the deleted run");

        var r4 = await ScanAsync("j", ("a", 1), ("b", 2), ("d", 4));
        Assert.AreEqual("a:1,b:2,d:4", await ViewAsync(r4));
        Assert.AreEqual(4, await RecordsAsync(), "run 4 extends run 2's records");
        Assert.AreEqual("a:1,b:2,d:4", await ViewAsync(r2));
    }

    [TestMethod]
    public async Task DeletingEveryRun_OfAJob_OrTheJobItself_RemovesAllItsRecords()
    {
        var (r1, r2, r3) = await SeriesAsync("gone");
        var other = await ScanAsync("kept", ("a", 1));
        await JobRepo.SaveAsync(new ComparisonJob { Id = "gone", Name = "gone" });
        await JobRepo.DeleteAsync("gone"); // deleting the job alone keeps its runs and their records

        Assert.AreEqual("a:1,b:5,d:4", await ViewAsync(r3));
        foreach (var r in new[] { r2, r1, r3 }) await DeleteAsync(r);
        Assert.AreEqual(1, await RecordsAsync(), "only the other job's record is left");
        Assert.AreEqual("a:1", await ViewAsync(other));
    }

    [TestMethod]
    public async Task Retention_DeletesOldRuns_WithoutChangingTheRunsItKeeps()
    {
        var (r1, r2, r3) = await SeriesAsync();
        var result = await _retention.RunOnceAsync(new RetentionOptions { KeepRunsPerJob = 1, KeepDays = 0, AuditLogDays = 0, BatchPause = TimeSpan.Zero }, DateTime.UtcNow);
        Assert.AreEqual(2, result.RunsDeleted);
        Assert.AreEqual("a:1,b:5,d:4", await ViewAsync(r3));
        Assert.AreEqual(3, await RecordsAsync());
        Assert.AreEqual(2, result.DifferencesDeleted, "c and the old b");
    }

    [TestMethod]
    public async Task ResettingAMiddleRun_SplitsTheSharedRecords_AndLeavesItsNeighboursAlone()
    {
        var r1 = await ScanAsync("j", ("a", 1));
        var r2 = await ScanAsync("j", ("a", 1));
        var r3 = await ScanAsync("j", ("a", 1));
        Assert.AreEqual(1, await RecordsAsync());

        await RunRepo.ResetRunDataAsync(r2.Id);
        Assert.AreEqual("a:1", await ViewAsync(r1));
        Assert.AreEqual("", await ViewAsync(r2));
        Assert.AreEqual("a:1", await ViewAsync(r3));
    }

    [TestMethod]
    public async Task ResumePreparation_RemovesOnlyTheRunsDifferencesAfterTheCheckpoint()
    {
        var r1 = await ScanAsync("j", ("a", 1), ("b", 2), ("c", 3));
        var r2 = await ScanAsync("j", ("a", 1), ("b", 2), ("c", 3));
        int removed = await RunRepo.PrepareResumeAsync(r2.Id, r2.Id, "a");
        Assert.AreEqual(2, removed);
        Assert.AreEqual("a:1", await ViewAsync(r2));
        Assert.AreEqual("a:1,b:2,c:3", await ViewAsync(r1));

        await DiffRepo.InsertBatchAsync(new[] { D(r2.Id, "b", 2), D(r2.Id, "c", 3) }); // the resumed scan finds them again
        Assert.AreEqual("a:1,b:2,c:3", await ViewAsync(r2));
        Assert.AreEqual(3, await RecordsAsync());
    }

    [TestMethod]
    public async Task AnInFlightDifference_ThatBecomesConfirmed_IsANewRecord()
    {
        var r1 = await ScanAsync("j");
        await DiffRepo.InsertBatchAsync(new[] { D(r1.Id, "a", inFlight: true) });
        var r2 = await ScanAsync("j");
        await DiffRepo.InsertBatchAsync(new[] { D(r2.Id, "a", inFlight: false) });
        Assert.AreEqual(1, await DiffRepo.GetDifferenceCountAsync(r1.Id, withinGracePeriodFilter: true));
        Assert.AreEqual(0, await DiffRepo.GetDifferenceCountAsync(r2.Id, withinGracePeriodFilter: true));
        Assert.AreEqual(1, await DiffRepo.GetDifferenceCountAsync(r2.Id, withinGracePeriodFilter: false));
    }

    [TestMethod]
    public async Task RunsFromBeforeDeduplication_StillReadAndDeleteTheOldWay()
    {
        var legacy = new ScanRun { JobId = "j", JobName = "j", Status = RunStatus.Completed, EndTimeUtc = DateTime.UtcNow };
        await RunRepo.CreateAsync(legacy);
        await RawScalarAsync<long>("UPDATE ScanRuns SET DiffSeq = NULL WHERE Id = @Id; SELECT 0;", new { legacy.Id });
        await DiffRepo.InsertBatchAsync(new[] { D(legacy.Id, "a", 1), D(legacy.Id, "b", 2) });
        Assert.AreEqual(2, await RawScalarAsync<long>("SELECT COUNT(*) FROM RecordedDifferences"));
        Assert.AreEqual("a:1,b:2", await ViewAsync(legacy));

        var next = await ScanAsync("j", ("a", 1));
        Assert.AreEqual("a:1", await ViewAsync(next));

        await DeleteAsync(legacy);
        Assert.AreEqual(0, await RawScalarAsync<long>("SELECT COUNT(*) FROM RecordedDifferences"));
        Assert.AreEqual("a:1", await ViewAsync(next));
    }
}
