using System.Diagnostics;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

/// <summary>
/// The partitioned, prefetching engine against <see cref="FakeS3Lister"/>: it must produce exactly what the single-stream path
/// produces, survive cancel/resume with several ranges in flight, resume old checkpoints, and honour the throughput limits.
/// </summary>
[TestClass]
public class ParallelScanTests : TestDbBase
{
    private static readonly DateTime OldDate = new(2025, 6, 1, 0, 0, 0, DateTimeKind.Utc);

    private static S3ObjectSummary Obj(string key, long size = 10, string etag = "e") => new() { Key = key, Size = size, ETag = etag, LastModified = OldDate, StorageClass = "STANDARD" };

    private static ComparisonJob NewJob(string prefixA = "", string prefixB = "") => new()
    {
        Name = "parallel",
        ReplicationGracePeriodMinutes = 0,
        DetectSequenceGaps = false,
        BucketA = { BucketName = "bucket-a", Prefix = prefixA },
        BucketB = { BucketName = "bucket-b", Prefix = prefixB }
    };

    private static ScanOptions Options(int partitions, int pageSize = 37) => new()
    {
        MaxParallelPartitions = partitions,
        ListPageSize = pageSize,
        PrefetchPages = 2,
        MaxListRequestsPerSecond = 0,
        MaxRecordedDifferencesPerRun = 0
    };

    private async Task<ScanRun> NewRunAsync(ComparisonJob job)
    {
        var run = new ScanRun { JobId = job.Id, JobName = job.Name };
        await RunRepo.CreateAsync(run);
        return run;
    }

    /// <summary>
    /// Random relative keys and the two buckets' contents: matches, missing in A/B, size and ETag mismatches, plus runs of
    /// consecutive missing keys long enough to span range boundaries.
    /// </summary>
    private static (List<S3ObjectSummary> A, List<S3ObjectSummary> B) Generate(int seed, List<string> relKeys, string prefixA = "", string prefixB = "")
    {
        var rng = new Random(seed);
        var keys = relKeys.Distinct().OrderBy(k => k, Utf8OrdinalComparer.Instance).ToList();
        var kind = new int[keys.Count]; // 0 same, 1 missing in B, 2 missing in A, 3 size, 4 etag
        for (int i = 0; i < keys.Count; i++)
        {
            int roll = rng.Next(100);
            kind[i] = roll < 70 ? 0 : roll < 78 ? 1 : roll < 86 ? 2 : roll < 93 ? 3 : 4;
        }
        for (int run = 0; run < 4; run++)
        {
            int start = rng.Next(keys.Count);
            int k = run % 2 == 0 ? 1 : 2;
            for (int i = start; i < Math.Min(keys.Count, start + keys.Count / 12); i++) kind[i] = k;
        }

        var a = new List<S3ObjectSummary>();
        var b = new List<S3ObjectSummary>();
        for (int i = 0; i < keys.Count; i++)
        {
            string key = keys[i];
            switch (kind[i])
            {
                case 0: a.Add(Obj(prefixA + key)); b.Add(Obj(prefixB + key)); break;
                case 1: a.Add(Obj(prefixA + key)); break;
                case 2: b.Add(Obj(prefixB + key)); break;
                case 3: a.Add(Obj(prefixA + key, 10)); b.Add(Obj(prefixB + key, 11)); break;
                default: a.Add(Obj(prefixA + key, 10, "e1")); b.Add(Obj(prefixB + key, 10, "e2")); break;
            }
        }
        return (a, b);
    }

    private static List<string> HierarchicalKeys(int dirs, int files) =>
        Enumerable.Range(0, dirs).SelectMany(d => Enumerable.Range(0, files).Select(f => $"d{d:00}/f{f:000}")).ToList();

    private static List<string> FlatKeys(int seed, int count)
    {
        var rng = new Random(seed);
        return Enumerable.Range(0, count).Select(_ => rng.Next().ToString("x8")).ToList();
    }

    private static List<string> UnicodeKeys()
    {
        string[] dirs = { "a", "ü", "日本", "！", "\U0001F600", "z", "Ω", "data" };
        return dirs.SelectMany(d => Enumerable.Range(0, 120).Select(i => $"{d}/{i:000}")).ToList();
    }

    private static List<(string Key, DifferenceType Type, long? SizeA, long? SizeB)> Shape(IEnumerable<RecordedDifference> diffs) =>
        diffs.Select(d => (d.Key, d.Type, d.BucketASize, d.BucketBSize))
            .OrderBy(t => t.Key, Utf8OrdinalComparer.Instance).ThenBy(t => t.Type).ToList();

    private static void AssertSameCounters(ScanRun expected, ScanRun actual)
    {
        Assert.AreEqual(expected.ObjectsCompared, actual.ObjectsCompared, nameof(ScanRun.ObjectsCompared));
        Assert.AreEqual(expected.MatchedCount, actual.MatchedCount, nameof(ScanRun.MatchedCount));
        Assert.AreEqual(expected.TotalDifferences, actual.TotalDifferences, nameof(ScanRun.TotalDifferences));
        Assert.AreEqual(expected.FilteredDifferencesCount, actual.FilteredDifferencesCount);
        Assert.AreEqual(expected.MissingInBucketACount, actual.MissingInBucketACount);
        Assert.AreEqual(expected.MissingInBucketBCount, actual.MissingInBucketBCount);
        Assert.AreEqual(expected.SizeMismatchCount, actual.SizeMismatchCount);
        Assert.AreEqual(expected.ETagMismatchCount, actual.ETagMismatchCount);
        Assert.AreEqual(expected.BucketAObjectsRead, actual.BucketAObjectsRead);
        Assert.AreEqual(expected.BucketBObjectsRead, actual.BucketBObjectsRead);
        Assert.AreEqual(expected.BucketABytesRead, actual.BucketABytesRead);
        Assert.AreEqual(expected.BucketBBytesRead, actual.BucketBBytesRead);
    }

    /// <summary>The reference: the original single-stream merge over in-memory streams.</summary>
    private async Task<(ScanRun Run, List<RecordedDifference> Diffs)> RunSingleStreamAsync(ComparisonJob job, List<S3ObjectSummary> a, List<S3ObjectSummary> b)
    {
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(a.Where(o => o.Key.StartsWith(job.BucketA.Prefix, StringComparison.Ordinal))),
            streamB: new MockS3ObjectStream(b.Where(o => o.Key.StartsWith(job.BucketB.Prefix, StringComparison.Ordinal))),
            options: new ScanOptions { MaxRecordedDifferencesPerRun = 0 });
        var finished = await engine.RunAsync();
        return (finished, await DiffRepo.GetAllForRunAsync(run.Id));
    }

    private async Task<(ScanRun Run, List<RecordedDifference> Diffs, ServiceComparisonEngine Engine)> RunPartitionedAsync(
        ComparisonJob job, IS3Lister a, IS3Lister b, ScanOptions options, ScanRun? run = null, ScanCheckpoint? checkpoint = null)
    {
        run ??= await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, checkpoint, options: options, listerA: a, listerB: b);
        var finished = await engine.RunAsync();
        return (finished, await DiffRepo.GetAllForRunAsync(run.Id), engine);
    }

    // ---------------- equivalence ----------------

    [TestMethod]
    [DataRow(1)]
    [DataRow(3)]
    [DataRow(17)]
    public async Task Partitioned_MatchesSingleStream_Hierarchical(int partitions)
    {
        var (a, b) = Generate(100 + partitions, HierarchicalKeys(40, 60));
        a.Add(Obj("d05/")); // a folder marker equal to a likely boundary (a common prefix), in A only
        b.Add(Obj("d20/"));
        var job = NewJob();
        var expected = await RunSingleStreamAsync(job, a, b);
        var actual = await RunPartitionedAsync(job, new FakeS3Lister(a), new FakeS3Lister(b), Options(partitions));

        if (partitions > 1) Assert.IsTrue(actual.Engine.GetPartitionsSnapshot().Count > 1, "the planner split the keyspace");
        else Assert.AreEqual(1, actual.Engine.GetPartitionsSnapshot().Count);
        Assert.IsTrue(actual.Engine.GetPartitionsSnapshot().All(r => r.Completed));
        AssertSameCounters(expected.Run, actual.Run);
        CollectionAssert.AreEqual(Shape(expected.Diffs), Shape(actual.Diffs));
        Assert.AreEqual(actual.Run.TotalDifferences, actual.Diffs.Count);
    }

    [TestMethod]
    [DataRow(3)]
    [DataRow(17)]
    public async Task Partitioned_MatchesSingleStream_Flat(int partitions)
    {
        var (a, b) = Generate(7, FlatKeys(9, 2500));
        var job = NewJob();
        var expected = await RunSingleStreamAsync(job, a, b);
        var actual = await RunPartitionedAsync(job, new FakeS3Lister(a), new FakeS3Lister(b), Options(partitions));

        Assert.IsTrue(actual.Engine.GetPartitionsSnapshot().Count > 1);
        AssertSameCounters(expected.Run, actual.Run);
        CollectionAssert.AreEqual(Shape(expected.Diffs), Shape(actual.Diffs));
    }

    [TestMethod]
    public async Task Partitioned_MatchesSingleStream_DifferentPrefixesAndUnicode()
    {
        var (a, b) = Generate(21, UnicodeKeys(), prefixA: "src/", prefixB: "replica/2024/");
        a.Add(Obj("other/ignored"));
        b.Add(Obj("replica/2023/ignored"));
        b.Add(Obj("replica/2024/")); // key equal to the prefix: empty relative key
        var job = NewJob("src/", "replica/2024/");
        var expected = await RunSingleStreamAsync(job, a, b);
        var actual = await RunPartitionedAsync(job, new FakeS3Lister(a), new FakeS3Lister(b), Options(5, pageSize: 13));

        Assert.IsTrue(actual.Engine.GetPartitionsSnapshot().Count > 1);
        AssertSameCounters(expected.Run, actual.Run);
        CollectionAssert.AreEqual(Shape(expected.Diffs), Shape(actual.Diffs));
        Assert.IsFalse(actual.Diffs.Any(d => d.Key.StartsWith("src/", StringComparison.Ordinal) || d.Key.Contains("ignored")), "keys are relative, outside keys ignored");
    }

    [TestMethod]
    public async Task Partitioned_DifferencesAreExportedInKeyOrder()
    {
        var (a, b) = Generate(5, HierarchicalKeys(30, 30));
        var job = NewJob();
        var actual = await RunPartitionedAsync(job, new FakeS3Lister(a, TimeSpan.FromMilliseconds(1)), new FakeS3Lister(b), Options(8, pageSize: 20));

        var streamed = new List<string>();
        await foreach (var d in DiffRepo.StreamAllForRunAsync(actual.Run.Id)) streamed.Add(d.Key);
        CollectionAssert.AreEqual(streamed.OrderBy(k => k, Utf8OrdinalComparer.Instance).ToList(), streamed);
        Assert.AreEqual(actual.Run.TotalDifferences, streamed.Count);

        // The export query is served by the (ScanRunId, Key) index without a separate sort.
        await using var conn = await Factory.CreateOpenConnectionAsync();
        var plan = string.Join(" | ", (await Dapper.SqlMapper.QueryAsync(conn, "EXPLAIN QUERY PLAN SELECT * FROM RecordedDifferences WHERE ScanRunId = 'x' ORDER BY Key ASC, Id ASC")).Select(r => (string)((IDictionary<string, object>)r)["detail"]));
        Assert.IsFalse(plan.Contains("TEMP B-TREE"), plan);
    }

    [TestMethod]
    public async Task Partitioned_ListerFailure_FailsTheRun_AndStopsAllWorkers()
    {
        var (a, b) = Generate(8, HierarchicalKeys(30, 40));
        var listerB = new FakeS3Lister(b) { Fault = call => call == 15 ? new InvalidOperationException("AccessDenied (simulated)") : null };
        var job = NewJob();
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(6, pageSize: 10), listerA: new FakeS3Lister(a), listerB: listerB);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => engine.RunAsync());
        StringAssert.Contains(ex.Message, "AccessDenied");
        var stored = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(RunStatus.Failed, stored.Status);
        var cp = await RunRepo.GetCheckpointAsync(run.Id);
        if (cp != null)
        {
            // Whatever the checkpoint claims is stored, exactly once.
            var keys = (await DiffRepo.GetAllForRunAsync(run.Id)).Select(d => d.Key).ToList();
            Assert.AreEqual(keys.Count, keys.Distinct().Count());
            Assert.AreEqual(cp.TotalDifferences, keys.Count);
        }
    }

    // ---------------- cancel / resume ----------------

    [TestMethod]
    public async Task CancelWithSeveralPartitionsInFlight_ThenResume_GivesTheUninterruptedResult()
    {
        var (a, b) = Generate(42, HierarchicalKeys(40, 50));
        var job = NewJob();
        var expected = await RunSingleStreamAsync(job, a, b);

        var listerA = new FakeS3Lister(a, TimeSpan.FromMilliseconds(2));
        var listerB = new FakeS3Lister(b, TimeSpan.FromMilliseconds(2));
        var run = await NewRunAsync(job);
        using var cts = new CancellationTokenSource();
        ServiceComparisonEngine? engine1 = null;
        int found = 0;
        engine1 = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(4, pageSize: 25), listerA: listerA, listerB: listerB);
        engine1.DifferenceFound += _ =>
        {
            if (Interlocked.Increment(ref found) == expected.Run.TotalDifferences / 2) { engine1!.MarkServiceShutdown(); cts.Cancel(); }
        };
        await Assert.ThrowsAsync<OperationCanceledException>(() => engine1.RunAsync(cts.Token));

        var interrupted = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(RunStatus.Interrupted, interrupted.Status);
        var cp = (await RunRepo.GetCheckpointAsync(run.Id))!;
        Assert.IsNotNull(cp.Partitions);
        Assert.IsTrue(cp.Partitions.Count > 1);
        Assert.IsTrue(cp.CanResume);
        Assert.IsTrue(cp.Partitions.Any(p => !p.Completed), "the scan was interrupted mid-way");
        Assert.AreEqual(cp.TotalDifferences, await DiffRepo.GetDifferenceCountAsync(run.Id), "checkpointed counters == stored differences");

        // Simulate a hard crash after the checkpoint: rows for keys past each range's checkpointed key reached the database.
        var leftovers = expected.Diffs
            .Where(d => { var r = cp.Partitions[PartitionPlanner.IndexOf(cp.Partitions, d.Key)]; return !r.Completed && (r.ResumeAfter == null || Utf8OrdinalComparer.CompareStatic(d.Key, r.ResumeAfter) > 0); })
            .Take(25)
            .Select(d => new RecordedDifference { ScanRunId = run.Id, Key = d.Key, Type = d.Type, Details = "leftover" })
            .ToList();
        Assert.IsTrue(leftovers.Count > 0);
        await DiffRepo.InsertBatchAsync(leftovers);

        var reopened = (await RunRepo.ReopenForResumeAsync(run.Id))!;
        var resumed = await RunPartitionedAsync(job, listerA, listerB, Options(4, pageSize: 25), reopened, cp);

        Assert.IsTrue(resumed.Engine.IsResuming);
        Assert.AreEqual(RunStatus.Completed, resumed.Run.Status);
        Assert.AreEqual(run.Id, resumed.Run.Id);
        Assert.AreEqual(cp.Partitions.Count, resumed.Engine.GetPartitionsSnapshot().Count, "the persisted plan is reused");
        AssertSameCounters(expected.Run, resumed.Run);
        CollectionAssert.AreEqual(Shape(expected.Diffs), Shape(resumed.Diffs));
        Assert.AreEqual(resumed.Run.TotalDifferences, resumed.Diffs.Count, "run counters match the stored differences");
        Assert.IsFalse(resumed.Diffs.Any(d => d.Details == "leftover"), "rows past the checkpoint were removed before resuming");
        Assert.IsNull(await RunRepo.GetCheckpointAsync(run.Id));
    }

    [TestMethod]
    public async Task OldSingleKeyCheckpoint_Resumes_FromItsKey()
    {
        var (a, b) = Generate(13, HierarchicalKeys(20, 40));
        var job = NewJob();
        var expected = await RunSingleStreamAsync(job, a, b);

        // Interrupt a single-stream scan, then store its checkpoint the way older versions did (no partition plan).
        var run = await NewRunAsync(job);
        using var cts = new CancellationTokenSource();
        ServiceComparisonEngine? engine1 = null;
        engine1 = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(a, onMoveNext: n => { if (n == 300) { engine1!.MarkServiceShutdown(); cts.Cancel(); } }),
            streamB: new MockS3ObjectStream(b));
        await Assert.ThrowsAsync<OperationCanceledException>(() => engine1.RunAsync(cts.Token));
        var cp = (await RunRepo.GetCheckpointAsync(run.Id))!;
        cp.Partitions = null;
        cp.Version = 1;
        await RunRepo.SaveCheckpointAsync(cp);
        Assert.IsNull(await RawScalarAsync<string>("SELECT PartitionPlanJson FROM Checkpoints WHERE ScanRunId = @Id", new { Id = run.Id }));

        var legacy = (await RunRepo.GetCheckpointAsync(run.Id))!;
        Assert.IsNull(legacy.Partitions);
        Assert.IsTrue(legacy.CanResume);

        var reopened = (await RunRepo.ReopenForResumeAsync(run.Id))!;
        var resumed = await RunPartitionedAsync(job, new FakeS3Lister(a), new FakeS3Lister(b), Options(8), reopened, legacy);

        Assert.IsTrue(resumed.Engine.IsResuming);
        var plan = resumed.Engine.GetPartitionsSnapshot();
        Assert.IsTrue(plan[0].Completed && plan[0].Upper == legacy.LastEvaluatedKey, "everything up to the old key was done");
        Assert.IsTrue(plan.Count > 2, "the rest of the keyspace was planned (see ParallelScanHardeningTests)");
        AssertSameCounters(expected.Run, resumed.Run);
        CollectionAssert.AreEqual(Shape(expected.Diffs), Shape(resumed.Diffs));
    }

    [TestMethod]
    public async Task PartitionedCheckpoint_WithSuppliedStreams_IsDiscarded()
    {
        var job = NewJob();
        var run = await NewRunAsync(job);
        var cp = new ScanCheckpoint
        {
            ScanRunId = run.Id, JobId = job.Id, BucketNameA = job.BucketA.BucketName, BucketNameB = job.BucketB.BucketName,
            EndpointA = job.BucketA.EndpointUrl, EndpointB = job.BucketB.EndpointUrl, Mode = job.Mode,
            Partitions = new() { new() { Upper = "m", Completed = true }, new() { Lower = "m" } }
        };
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, cp, new MockS3ObjectStream(new[] { Obj("a") }), new MockS3ObjectStream(new[] { Obj("a") }));
        Assert.IsFalse(engine.IsResuming);
        StringAssert.Contains(engine.CheckpointDiscardReason, "supplied streams");

        var broken = new ScanCheckpoint
        {
            ScanRunId = run.Id, JobId = job.Id, Mode = job.Mode,
            Partitions = new() { new() { Upper = "m", Completed = true }, new() { Lower = "n" } }
        };
        var engine2 = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, broken, options: Options(4), listerA: new FakeS3Lister(Array.Empty<S3ObjectSummary>()), listerB: new FakeS3Lister(Array.Empty<S3ObjectSummary>()));
        Assert.IsFalse(engine2.IsResuming);
        StringAssert.Contains(engine2.CheckpointDiscardReason, "partition plan is invalid");
    }

    [TestMethod]
    public async Task PrepareResume_PerRange_DeletesOnlyPastEachRangesCheckpoint()
    {
        var run = new ScanRun { JobId = "j", JobName = "n" };
        await RunRepo.CreateAsync(run);
        await DiffRepo.InsertBatchAsync(new[] { "a", "c", "d", "m", "n", "s", "t", "u", "z" }.Select(k =>
            new RecordedDifference { ScanRunId = run.Id, Key = k, Type = DifferenceType.MissingInBucketB }));

        var plan = new List<PartitionRange>
        {
            new() { Upper = "m", LastCompletedKey = "c" },      // keep a, c; drop d, m
            new() { Lower = "m", Upper = "t", Completed = true }, // keep n, s, t
            new() { Lower = "t" }                                 // nothing done: drop u, z
        };
        int removed = await RunRepo.PrepareResumeAsync(run.Id, run.Id, plan);

        Assert.AreEqual(4, removed);
        CollectionAssert.AreEqual(new[] { "a", "c", "n", "s", "t" }, (await DiffRepo.GetAllForRunAsync(run.Id)).Select(d => d.Key).ToList());
    }

    [TestMethod]
    public async Task Checkpoint_PlanRoundTrips_ThroughItsOwnColumn()
    {
        var run = new ScanRun { JobId = "j", JobName = "n" };
        await RunRepo.CreateAsync(run);
        var plan = new List<PartitionRange> { new() { Upper = "k", Completed = true, LastCompletedKey = "j" }, new() { Lower = "k", LastCompletedKey = "" } };
        await RunRepo.SaveCheckpointAsync(new ScanCheckpoint { ScanRunId = run.Id, JobId = "j", Version = 2, Partitions = plan });

        var json = await RawScalarAsync<string>("SELECT PartitionPlanJson FROM Checkpoints WHERE ScanRunId = @Id", new { Id = run.Id });
        StringAssert.Contains(json, "\"Upper\":\"k\"");
        Assert.IsFalse((await RawScalarAsync<string>("SELECT CheckpointJson FROM Checkpoints WHERE ScanRunId = @Id", new { Id = run.Id }))!.Contains("Partitions"));

        var loaded = (await RunRepo.GetCheckpointAsync(run.Id))!;
        Assert.AreEqual(2, loaded.Partitions!.Count);
        Assert.IsTrue(loaded.Partitions[0].Completed);
        Assert.AreEqual("", loaded.Partitions[1].LastCompletedKey, "an empty relative key is progress, not 'none'");
        Assert.IsTrue(loaded.CanResume);
    }

    [TestMethod]
    public async Task Version3Database_GetsPlanAndPerformanceColumns_AndOldCheckpointsStillLoad()
    {
        var run = new ScanRun { JobId = "j", JobName = "n" };
        await RunRepo.CreateAsync(run);
        await using (var conn = await Factory.CreateOpenConnectionAsync())
        {
            await Dapper.SqlMapper.ExecuteAsync(conn, "ALTER TABLE Checkpoints DROP COLUMN PartitionPlanJson; ALTER TABLE Jobs DROP COLUMN Performance_Json; PRAGMA user_version = 3;");
            await Dapper.SqlMapper.ExecuteAsync(conn, "INSERT INTO Checkpoints (ScanRunId, JobId, CheckpointJson, LastUpdatedAtUtc) VALUES (@Id, 'j', @Json, '2026-01-01T00:00:00.0000000Z')",
                new { Id = run.Id, Json = "{\"Version\":1,\"ScanRunId\":\"" + run.Id + "\",\"LastEvaluatedKey\":\"k/0042\",\"HasLastEvaluatedKey\":true,\"ObjectsCompared\":43}" });
        }

        await new DatabaseMigrator(Factory).MigrateAsync();
        Assert.AreEqual(DatabaseMigrator.LatestVersion, await RawScalarAsync<int>("PRAGMA user_version"));

        var cp = (await RunRepo.GetCheckpointAsync(run.Id))!;
        Assert.IsNull(cp.Partitions);
        Assert.AreEqual("k/0042", cp.LastEvaluatedKey);
        Assert.IsTrue(cp.CanResume);

        var job = new ComparisonJob { Name = "perf" };
        job.Performance.MaxParallelPartitions = 4;
        await JobRepo.SaveAsync(job);
        Assert.AreEqual(4, (await JobRepo.GetByIdAsync(job.Id))!.Performance.MaxParallelPartitions);
        var plain = new ComparisonJob { Name = "plain" };
        await JobRepo.SaveAsync(plain);
        Assert.IsNull(await RawScalarAsync<string>("SELECT Performance_Json FROM Jobs WHERE Id = @Id", new { plain.Id }), "no overrides = NULL");
        Assert.IsTrue((await JobRepo.GetByIdAsync(plain.Id))!.Performance.IsEmpty);
    }

    // ---------------- limits and throughput ----------------

    [TestMethod]
    public async Task MaxObjectsPerSecond_IsApproximatelyRespected_AcrossPartitions()
    {
        var keys = HierarchicalKeys(20, 100); // 2,000 identical keys
        var items = keys.Select(k => Obj(k)).ToList();
        var job = NewJob();
        var options = Options(4, pageSize: 100);
        options.MaxObjectsPerSecond = 1000; // burst of 1,000, then 1,000/s: about 1 s for 2,000 keys

        var sw = Stopwatch.StartNew();
        var result = await RunPartitionedAsync(job, new FakeS3Lister(items), new FakeS3Lister(items), options);
        sw.Stop();

        Assert.AreEqual(2000, result.Run.ObjectsCompared);
        Assert.IsTrue(sw.Elapsed >= TimeSpan.FromMilliseconds(600), $"too fast for the cap: {sw.ElapsedMilliseconds} ms");
        Assert.IsTrue(sw.Elapsed < TimeSpan.FromSeconds(30), $"much slower than the cap: {sw.ElapsedMilliseconds} ms");
    }

    [TestMethod]
    public async Task MaxListRequestsPerSecond_IsSharedByAllPartitions()
    {
        var items = HierarchicalKeys(10, 10).Select(k => Obj(k)).ToList(); // 100 keys, one per page: 100+ requests per bucket
        var job = NewJob();
        var options = Options(8, pageSize: 1);
        options.PartitionOversubscription = 1; // 8 ranges from the 10 directories: no probing
        options.MaxListRequestsPerSecond = 50;
        var listerA = new FakeS3Lister(items);

        var sw = Stopwatch.StartNew();
        await RunPartitionedAsync(job, listerA, new FakeS3Lister(items), options);
        sw.Stop();

        // Burst of 50, then 50/s for the rest of bucket A's requests (planner + pages), whatever the partition count.
        double minSeconds = Math.Max(0, listerA.Calls - 50) / 50.0;
        Assert.IsTrue(listerA.Calls > 80);
        Assert.IsTrue(sw.Elapsed.TotalSeconds >= minSeconds * 0.6, $"{listerA.Calls} requests in {sw.ElapsedMilliseconds} ms");
    }

    [TestMethod]
    [DoNotParallelize] // a timing comparison: run it alone, after the parallel tests
    public async Task PartitionedWithPrefetch_IsSeveralTimesFaster_ThanOnePartition()
    {
        var keys = HierarchicalKeys(30, 40); // 1,200 keys, 30 directories
        var items = keys.Select(k => Obj(k)).ToList();
        var latency = TimeSpan.FromMilliseconds(25);
        var job = NewJob();

        var single = Options(1, pageSize: 30);
        var sw = Stopwatch.StartNew();
        var r1 = await RunPartitionedAsync(job, new FakeS3Lister(items, latency), new FakeS3Lister(items, latency), single);
        var tSingle = sw.Elapsed;

        var parallel = Options(8, pageSize: 30);
        parallel.PartitionOversubscription = 2; // 16 ranges from the 30 directories: one cheap '/' listing per bucket
        var listerA = new FakeS3Lister(items, latency);
        sw.Restart();
        var r8 = await RunPartitionedAsync(job, listerA, new FakeS3Lister(items, latency), parallel);
        var tParallel = sw.Elapsed;

        Console.WriteLine($"1 partition: {tSingle.TotalMilliseconds:N0} ms, 8 partitions: {tParallel.TotalMilliseconds:N0} ms");
        Assert.AreEqual(1200, r1.Run.MatchedCount);
        Assert.AreEqual(1200, r8.Run.MatchedCount);
        Assert.IsTrue(listerA.MaxInFlight > 1, "bucket A was listed by several partitions at once");
        Assert.IsTrue(tParallel.TotalMilliseconds * 2 < tSingle.TotalMilliseconds,
            $"1 partition: {tSingle.TotalMilliseconds:N0} ms, 8 partitions: {tParallel.TotalMilliseconds:N0} ms");
    }

    [TestMethod]
    public async Task Prefetch_ListsBothBucketsConcurrently_InOnePartition()
    {
        var items = HierarchicalKeys(5, 40).Select(k => Obj(k)).ToList();
        var latency = TimeSpan.FromMilliseconds(25);
        int inFlight = 0, maxInFlight = 0;
        void Track(int _) { int n = Interlocked.Increment(ref inFlight); int m; while (n > (m = maxInFlight) && Interlocked.CompareExchange(ref maxInFlight, n, m) != m) { } }
        var listerA = new FakeS3Lister(items, latency) { OnCall = Track };
        var listerB = new FakeS3Lister(items, latency) { OnCall = Track };
        listerA.Fault = listerB.Fault = _ => { Interlocked.Decrement(ref inFlight); return null; };

        var result = await RunPartitionedAsync(NewJob(), listerA, listerB, Options(1, pageSize: 20));
        Assert.AreEqual(200, result.Run.MatchedCount);
        Assert.IsTrue(maxInFlight >= 2, "A and B requests overlapped");
    }

    [TestMethod]
    public async Task PauseGatesAllWorkers_AndProgressReportsPartitions()
    {
        var items = HierarchicalKeys(30, 30).Select(k => Obj(k)).ToList();
        var job = NewJob();
        var run = await NewRunAsync(job);
        var listerA = new FakeS3Lister(items, TimeSpan.FromMilliseconds(2));
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(4, pageSize: 15), listerA: listerA, listerB: new FakeS3Lister(items, TimeSpan.FromMilliseconds(2)));
        ComparisonStatistics? last = null;
        engine.ProgressUpdated += s => last = s;

        var task = engine.RunAsync();
        for (int i = 0; i < 200 && listerA.Calls < 10; i++) await Task.Delay(5);
        engine.Pause();
        await Task.Delay(150);
        int calls = listerA.Calls;
        await Task.Delay(300);
        Assert.IsFalse(task.IsCompleted);
        // Producers may top up their bounded buffers, but nobody keeps listing while paused.
        Assert.IsTrue(listerA.Calls - calls <= 4 * 4, $"listing went on while paused: {calls} -> {listerA.Calls}");

        engine.Resume();
        var finished = await task;
        Assert.AreEqual(900, finished.MatchedCount);
        Assert.IsNotNull(last);
        Assert.IsTrue(last.PartitionsTotal > 1);
        Assert.AreEqual(last.PartitionsTotal, last.PartitionsCompleted);
        Assert.AreEqual(900, last.BucketAObjectsRead);
    }

    [TestMethod]
    public void ScanOptions_ForJob_AppliesOnlySetOverrides_AndValidates()
    {
        var service = new ScanOptions { MaxParallelPartitions = 8, MaxListRequestsPerSecond = 20, MaxObjectsPerSecond = 0 };
        var job = new ComparisonJob();
        job.Performance.MaxParallelPartitions = 2;
        job.Performance.MaxObjectsPerSecond = 500;
        var effective = service.ForJob(job);
        Assert.AreEqual(2, effective.MaxParallelPartitions);
        Assert.AreEqual(500, effective.MaxObjectsPerSecond);
        Assert.AreEqual(20, effective.MaxListRequestsPerSecond, "unset = service default");
        Assert.AreEqual(8, service.MaxParallelPartitions, "the service options are not changed");
        Assert.AreEqual(8, service.ForJob(new ComparisonJob { Performance = null! }).MaxParallelPartitions);

        Assert.IsNull(ScanOptions.Validate(job.Performance));
        Assert.IsNull(ScanOptions.Validate(new JobPerformanceSettings()));
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { MaxParallelPartitions = 0 }));
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { MaxParallelPartitions = 257 }));
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { PrefetchPages = 0 }));
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { MaxListRequestsPerSecond = double.NaN }));
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { MaxObjectsPerSecond = -1 }));
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { MaxConcurrentContentChecks = 0 }));
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { MaxDiscoveryRequests = -1 }));
        Assert.IsNull(ScanOptions.Validate(new JobPerformanceSettings { MaxDiscoveryRequests = 0, MaxListRequestsPerSecond = 0, MaxObjectsPerSecond = 0 }));
    }

    [TestMethod]
    public void TokenBucket_BatchAcquire_TakesSeveralTokensAtOnce()
    {
        var time = new ManualTime();
        var limiter = new TokenBucketRateLimiter(100, time);
        Assert.AreEqual(TimeSpan.Zero, limiter.TryAcquire(60));
        Assert.AreEqual(TimeSpan.Zero, limiter.TryAcquire(40));
        Assert.AreEqual(0.5, limiter.TryAcquire(50).TotalSeconds, 1e-6, "50 tokens at 100/s");
        time.Advance(TimeSpan.FromSeconds(0.5));
        Assert.AreEqual(TimeSpan.Zero, limiter.TryAcquire(50));
        Assert.AreEqual(1.0, limiter.TryAcquire(5000).TotalSeconds, 1e-6, "a request above the capacity is capped to it");
    }

    private sealed class ManualTime : TimeProvider
    {
        private long _ticks = 1;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _ticks;
        public void Advance(TimeSpan by) => _ticks += by.Ticks;
    }
}
