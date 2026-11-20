using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;
using Amazon.S3;
using Microsoft.Extensions.Configuration;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;
using S3CompareService.Worker;

namespace S3CompareService.Tests;

/// <summary>
/// Review follow-ups for the parallel scanner: object throttling over small ranges, Full Data read errors per key, sequence
/// gaps across range boundaries, cancellation surfacing as other exceptions, single-key checkpoint resume in parallel,
/// memory and configuration limits, corrupt job settings, and events raised without a lock.
/// </summary>
[TestClass]
public class ParallelScanHardeningTests : TestDbBase
{
    private static readonly DateTime OldDate = new(2025, 6, 1, 0, 0, 0, DateTimeKind.Utc);

    private static S3ObjectSummary Obj(string key, long size = 10, string etag = "e") => new() { Key = key, Size = size, ETag = etag, LastModified = OldDate, StorageClass = "STANDARD" };

    private static ComparisonJob NewJob(bool gaps = false) => new()
    {
        Name = "hardening",
        ReplicationGracePeriodMinutes = 0,
        DetectSequenceGaps = gaps,
        MaxSequenceGapThreshold = 25,
        BucketA = { BucketName = "bucket-a" },
        BucketB = { BucketName = "bucket-b" }
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

    private static List<(string Key, DifferenceType Type)> Shape(IEnumerable<RecordedDifference> diffs) =>
        diffs.Select(d => (d.Key, d.Type)).OrderBy(t => t.Key, Utf8OrdinalComparer.Instance).ThenBy(t => t.Type).ToList();

    // ---------------- 1. MaxObjectsPerSecond over ranges smaller than a token batch ----------------

    [TestMethod]
    public async Task MaxObjectsPerSecond_IsCharged_WhenRangesAreSmallerThanTheTokenBatch()
    {
        var keys = Enumerable.Range(0, 100).SelectMany(d => Enumerable.Range(0, 6).Select(f => $"d{d:000}/f{f}")).ToList(); // 600 keys
        var items = keys.Select(k => Obj(k)).ToList();
        var options = Options(8, pageSize: 100);
        options.PartitionOversubscription = 13; // about 100 ranges of 6 keys; the token batch is 300 / 20 = 15 keys
        options.MaxObjectsPerSecond = 300;      // burst of 300, then 300/s: about 1 s for 600 keys

        var job = NewJob();
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: options, listerA: new FakeS3Lister(items), listerB: new FakeS3Lister(items));
        var sw = Stopwatch.StartNew();
        var finished = await engine.RunAsync();
        sw.Stop();

        Assert.AreEqual(600, finished.MatchedCount);
        Assert.IsTrue(engine.GetPartitionsSnapshot().Count > 50, $"small ranges: {engine.GetPartitionsSnapshot().Count}");
        Assert.IsTrue(sw.Elapsed >= TimeSpan.FromMilliseconds(600), $"the object limit was not applied: {sw.ElapsedMilliseconds} ms");
        Assert.IsTrue(sw.Elapsed < TimeSpan.FromSeconds(20), $"much slower than the limit: {sw.ElapsedMilliseconds} ms");
    }

    // ---------------- 4. Full Data: read errors are per key ----------------

    /// <summary>A lister that can also read object content: <see cref="Open"/> gets the key and the 1-based attempt for it.</summary>
    private sealed class ContentLister : IS3Lister, IS3ObjectReader
    {
        private readonly FakeS3Lister _inner;
        private readonly ConcurrentDictionary<string, int> _attempts = new();
        public ContentLister(IEnumerable<S3ObjectSummary> items) { _inner = new FakeS3Lister(items); }
        public required Func<string, int, Stream> Open { get; init; }
        public int Attempts(string key) => _attempts.TryGetValue(key, out var n) ? n : 0;

        public Task<S3ListPage> ListPageAsync(string bucket, string? prefix, string? startAfter, string? continuationToken, string? delimiter, int maxKeys, CancellationToken cancellationToken)
            => _inner.ListPageAsync(bucket, prefix, startAfter, continuationToken, delimiter, maxKeys, cancellationToken);

        public async Task<Stream> OpenReadAsync(string bucket, string key, CancellationToken cancellationToken)
        {
            await Task.Yield();
            int attempt = _attempts.AddOrUpdate(key, 1, (_, n) => n + 1);
            return Open(key, attempt);
        }
    }

    private sealed class FailingReadStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new IOException("connection reset by peer (simulated)");
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => throw new IOException("connection reset by peer (simulated)");
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private static Stream Text(string s) => new MemoryStream(Encoding.UTF8.GetBytes(s));

    private static AmazonS3Exception S3Error(string code, HttpStatusCode status) => new($"{code} (simulated)") { ErrorCode = code, StatusCode = status };

    [TestMethod]
    public async Task FullData_ReadErrors_AreRecordedPerKey_AndDoNotAbortTheScan()
    {
        // Every key has different ETags, so every key's content is read.
        var keys = new[] { "k0-same", "k1-differs", "k2-denied", "k3-503-once", "k4-deleted", "k5-io", "k6-timeout", "k7-same" };
        var a = keys.Select(k => Obj(k, 5, "ea")).ToList();
        var b = keys.Select(k => Obj(k, 5, "eb")).ToList();

        var listerA = new ContentLister(a)
        {
            Open = (key, attempt) => key switch
            {
                "k2-denied" => throw S3Error("AccessDenied", HttpStatusCode.Forbidden),
                "k4-deleted" => throw S3Error("NoSuchKey", HttpStatusCode.NotFound),
                "k6-timeout" => throw new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout (simulated)."),
                _ => Text("hello")
            }
        };
        var listerB = new ContentLister(b)
        {
            Open = (key, attempt) => key switch
            {
                "k1-differs" => Text("HELLO"),
                "k3-503-once" when attempt == 1 => throw S3Error("ServiceUnavailable", HttpStatusCode.ServiceUnavailable),
                "k5-io" => new FailingReadStream(),
                _ => Text("hello")
            }
        };

        var job = NewJob();
        job.Mode = ComparisonMode.FullData;
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(3, pageSize: 2), listerA: listerA, listerB: listerB);
        var finished = await engine.RunAsync();

        Assert.AreEqual(RunStatus.Completed, finished.Status);
        Assert.AreEqual(8, finished.ObjectsCompared);
        Assert.AreEqual(3, finished.MatchedCount, "k0, k7 and k3 (after one retry)");
        Assert.AreEqual(1, finished.ContentMismatchCount);
        Assert.AreEqual(3, finished.ContentCheckErrorCount, "denied, I/O error and timeout; the deleted object is in-flight");
        Assert.AreEqual(1, finished.InFlightDifferencesCount);
        Assert.AreEqual(4, finished.FilteredDifferencesCount);

        var diffs = (await DiffRepo.GetAllForRunAsync(run.Id)).ToDictionary(d => d.Key);
        Assert.AreEqual(DifferenceType.ContentMismatch, diffs["k1-differs"].Type);
        Assert.AreEqual(DifferenceType.ContentCheckError, diffs["k2-denied"].Type);
        StringAssert.Contains(diffs["k2-denied"].Details, "AccessDenied");
        Assert.IsFalse(diffs["k2-denied"].IsWithinGracePeriod);
        Assert.IsTrue(diffs["k4-deleted"].IsWithinGracePeriod, "deleted during the scan = in-flight");
        StringAssert.Contains(diffs["k4-deleted"].Details, "NoSuchKey");
        StringAssert.Contains(diffs["k5-io"].Details, "after one retry");
        StringAssert.Contains(diffs["k6-timeout"].Details, "timed out");
        Assert.IsFalse(diffs.ContainsKey("k3-503-once"));

        Assert.AreEqual(1, listerA.Attempts("k2-denied"), "access denied is not retried");
        Assert.AreEqual(2, listerB.Attempts("k3-503-once"), "a 503 is retried once");
        Assert.AreEqual(2, listerB.Attempts("k5-io"), "an I/O error is retried once");
        Assert.AreEqual(1, listerA.Attempts("k4-deleted"));

        var alerts = Core.Notifications.AlertNotificationService.EvaluateDisparityThresholds(new JobAlertConfig { MaxDisparitiesThreshold = 100, MaxContentCheckErrorThreshold = 2 }, finished);
        Assert.IsTrue(alerts.Any(r => r.Contains("Content Check Errors")), string.Join("; ", alerts));
        var stored = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(3, stored.ContentCheckErrorCount, "the counter is persisted");
    }

    [TestMethod]
    public async Task FullData_ScanCancellation_DuringARead_CancelsTheRun()
    {
        var keys = Enumerable.Range(0, 40).Select(i => $"k{i:00}").ToList();
        using var cts = new CancellationTokenSource();
        var listerA = new ContentLister(keys.Select(k => Obj(k, 5, "ea"))) { Open = (_, _) => Text("hello") };
        var listerB = new ContentLister(keys.Select(k => Obj(k, 5, "eb")))
        {
            Open = (key, _) =>
            {
                if (key == "k20") { cts.Cancel(); throw new IOException("aborted by cancellation (simulated)"); }
                return Text("hello");
            }
        };
        var job = NewJob();
        job.Mode = ComparisonMode.FullData;
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(1), listerA: listerA, listerB: listerB);

        await Assert.ThrowsAsync<OperationCanceledException>(() => engine.RunAsync(cts.Token));
        Assert.AreEqual(RunStatus.Cancelled, (await RunRepo.GetByIdAsync(run.Id))!.Status);
        Assert.IsFalse((await DiffRepo.GetAllForRunAsync(run.Id)).Any(d => d.Key == "k20"), "the cancelled key is not recorded as a read error");
    }

    // ---------------- 5. sequence gaps across range boundaries ----------------

    private async Task<ScanRun> RunSingleStreamAsync(ComparisonJob job, List<S3ObjectSummary> a, List<S3ObjectSummary> b, List<RecordedDifference> diffsOut)
    {
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(a), streamB: new MockS3ObjectStream(b), options: new ScanOptions { MaxRecordedDifferencesPerRun = 0 });
        var finished = await engine.RunAsync();
        diffsOut.AddRange(await DiffRepo.GetAllForRunAsync(run.Id));
        return finished;
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task SequenceGaps_WithPartitions_MatchTheSingleStreamScan(bool hierarchical)
    {
        // Numbered series with holes (keys missing on both sides), long enough that range boundaries fall inside them.
        var rng = new Random(hierarchical ? 4 : 5);
        var all = hierarchical
            ? Enumerable.Range(0, 12).SelectMany(d => Enumerable.Range(0, 150).Select(n => $"cam{d:00}/img_{n:00000}.jpg")).ToList()
            : Enumerable.Range(0, 2400).Select(n => $"frame_{n:000000}.raw").ToList();
        var present = all.Where(_ => rng.Next(100) >= 6).ToList();
        var a = present.Where(_ => rng.Next(100) >= 3).Select(k => Obj(k)).ToList(); // some only in B
        var b = present.Where(k => a.All(o => o.Key != k) || rng.Next(100) >= 3).Select(k => Obj(k)).ToList();

        var job = NewJob(gaps: true);
        var expectedDiffs = new List<RecordedDifference>();
        var expected = await RunSingleStreamAsync(job, a, b, expectedDiffs);
        Assert.IsTrue(expected.SequenceGapCount > 20, $"the data has gaps: {expected.SequenceGapCount}");

        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(6, pageSize: 50), listerA: new FakeS3Lister(a), listerB: new FakeS3Lister(b));
        var actual = await engine.RunAsync();

        Assert.IsTrue(engine.GetPartitionsSnapshot().Count > 4, string.Join(" ", engine.GetPartitionsSnapshot()));
        Assert.AreEqual(expected.SequenceGapCount, actual.SequenceGapCount);
        Assert.AreEqual(expected.TotalDifferences, actual.TotalDifferences);
        CollectionAssert.AreEqual(Shape(expectedDiffs), Shape(await DiffRepo.GetAllForRunAsync(run.Id)));
    }

    // ---------------- 6. cancellation that surfaces as another exception ----------------

    /// <summary>Cancels the scan on the first insert and then fails that insert with a non-cancellation error.</summary>
    private sealed class CancelOnInsertDiffRepo : IDifferenceRepository
    {
        private readonly IDifferenceRepository _inner;
        private readonly CancellationTokenSource _cts;
        private int _calls;
        public CancelOnInsertDiffRepo(IDifferenceRepository inner, CancellationTokenSource cts) { _inner = inner; _cts = cts; }

        public Task InsertBatchAsync(IEnumerable<RecordedDifference> differences, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _calls) == 1)
            {
                _cts.Cancel();
                throw new InvalidOperationException("connection closed while cancelling (simulated)");
            }
            return _inner.InsertBatchAsync(differences, CancellationToken.None);
        }
        public Task<List<RecordedDifference>> GetDifferencesAsync(string scanRunId, DifferenceType? typeFilter = null, bool? withinGracePeriodFilter = null, string? searchKey = null, int skip = 0, int take = 50, CancellationToken cancellationToken = default)
            => _inner.GetDifferencesAsync(scanRunId, typeFilter, withinGracePeriodFilter, searchKey, skip, take, cancellationToken);
        public Task<int> GetDifferenceCountAsync(string scanRunId, DifferenceType? typeFilter = null, bool? withinGracePeriodFilter = null, string? searchKey = null, CancellationToken cancellationToken = default)
            => _inner.GetDifferenceCountAsync(scanRunId, typeFilter, withinGracePeriodFilter, searchKey, cancellationToken);
        public Task<List<RecordedDifference>> GetAllForRunAsync(string scanRunId, bool? withinGracePeriodFilter = null, CancellationToken cancellationToken = default)
            => _inner.GetAllForRunAsync(scanRunId, withinGracePeriodFilter, cancellationToken);
        public IAsyncEnumerable<RecordedDifference> StreamAllForRunAsync(string scanRunId, bool? withinGracePeriodFilter = null, CancellationToken cancellationToken = default)
            => _inner.StreamAllForRunAsync(scanRunId, withinGracePeriodFilter, cancellationToken);
    }

    [TestMethod]
    public async Task NonCancellationException_WhileCancelling_EndsAsCancelled_NotFailed()
    {
        var a = Enumerable.Range(0, 60).Select(i => Obj($"k{i:00}")).ToList();
        var b = a.Where((_, i) => i % 6 != 0).ToList(); // 10 differences: stored by the final flush after the workers
        using var cts = new CancellationTokenSource();
        var job = NewJob();
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, new CancelOnInsertDiffRepo(DiffRepo, cts), AuditRepo, options: Options(3), listerA: new FakeS3Lister(a), listerB: new FakeS3Lister(b));

        var ex = await Assert.ThrowsAsync<OperationCanceledException>(() => engine.RunAsync(cts.Token));
        Assert.IsInstanceOfType<InvalidOperationException>(ex.InnerException);
        var stored = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(RunStatus.Cancelled, stored.Status);
    }

    // ---------------- 8. a single-key checkpoint resumes in parallel ----------------

    [TestMethod]
    public async Task SingleKeyCheckpoint_PlansTheRestOfTheKeyspace_AndResumesInParallel()
    {
        var keys = Enumerable.Range(0, 30).SelectMany(d => Enumerable.Range(0, 40).Select(f => $"d{d:00}/f{f:000}")).ToList();
        var a = keys.Select(k => Obj(k)).ToList();
        var b = keys.Where((_, i) => i % 7 != 0).Select(k => Obj(k)).ToList();
        var job = NewJob();
        var expectedDiffs = new List<RecordedDifference>();
        var expected = await RunSingleStreamAsync(job, a, b, expectedDiffs);

        // Interrupt a single-stream scan and keep its checkpoint as older versions stored it (one key, no plan).
        var run = await NewRunAsync(job);
        using var cts = new CancellationTokenSource();
        ServiceComparisonEngine? engine1 = null;
        engine1 = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(a, onMoveNext: n => { if (n == 400) { engine1!.MarkServiceShutdown(); cts.Cancel(); } }),
            streamB: new MockS3ObjectStream(b));
        await Assert.ThrowsAsync<OperationCanceledException>(() => engine1.RunAsync(cts.Token));
        var cp = (await RunRepo.GetCheckpointAsync(run.Id))!;
        cp.Partitions = null;
        cp.Version = 1;
        await RunRepo.SaveCheckpointAsync(cp);
        var legacy = (await RunRepo.GetCheckpointAsync(run.Id))!;
        Assert.IsNull(legacy.Partitions);

        var listerA = new FakeS3Lister(a);
        var reopened = (await RunRepo.ReopenForResumeAsync(run.Id))!;
        var engine = new ServiceComparisonEngine(job, reopened, RunRepo, DiffRepo, AuditRepo, legacy, options: Options(6, pageSize: 20), listerA: listerA, listerB: new FakeS3Lister(b));
        var resumed = await engine.RunAsync();

        var plan = engine.GetPartitionsSnapshot();
        Assert.IsNull(PartitionPlanner.ValidateTiling(plan));
        Assert.IsTrue(plan.Count > 3, $"the rest of the keyspace was split: {string.Join(" ", plan)}");
        Assert.AreEqual(legacy.LastEvaluatedKey, plan[0].Upper, "the first range is what the old checkpoint had done");
        Assert.AreEqual(legacy.LastEvaluatedKey, plan[1].Lower);
        Assert.IsTrue(listerA.MaxInFlight > 1, "several ranges were listed at once");
        Assert.AreEqual(expected.ObjectsCompared, resumed.ObjectsCompared);
        Assert.AreEqual(expected.MatchedCount, resumed.MatchedCount);
        Assert.AreEqual(expected.TotalDifferences, resumed.TotalDifferences);
        CollectionAssert.AreEqual(Shape(expectedDiffs), Shape(await DiffRepo.GetAllForRunAsync(run.Id)));
    }

    [TestMethod]
    public void ResumePlanAfter_TilesTheKeyspace_WithTheDonePartFirst()
    {
        var plan = PartitionPlanner.BuildRanges(new[] { "c", "f", "m", "t" }, 5);
        var resumed = PartitionPlanner.ResumePlanAfter(plan, "g");
        Assert.IsNull(PartitionPlanner.ValidateTiling(resumed), string.Join(" ", resumed));
        Assert.AreEqual("(null..g] done, (g..m], (m..t], (t..null)",
            string.Join(", ", resumed.Select(r => $"({r.Lower ?? "null"}..{r.Upper ?? "null"}{(r.Upper == null ? ")" : "]")}{(r.Completed ? " done" : "")}")));
        Assert.AreEqual((true, "g"), PartitionPlanner.Frontier(resumed));

        var past = PartitionPlanner.ResumePlanAfter(plan, "z");
        Assert.AreEqual(2, past.Count);
        Assert.IsNull(PartitionPlanner.ValidateTiling(past));
    }

    // ---------------- 9 / 11. memory cap and configuration limits ----------------

    [TestMethod]
    public void MemoryCap_LimitsPartitionsTimesPrefetch()
    {
        var service = new ScanOptions { MaxParallelPartitions = 8, PrefetchPages = 64 };
        var job = new ComparisonJob();
        job.Performance.MaxParallelPartitions = 256;
        var effective = service.ForJob(job);
        Assert.AreEqual(256, effective.MaxParallelPartitions);
        Assert.AreEqual(1, effective.PrefetchPages, "256 x (1 + 1) = 512 pages per bucket");
        Assert.IsTrue((long)effective.MaxParallelPartitions * (effective.PrefetchPages + 1) <= ScanOptions.MaxBufferedPagesPerBucket);

        Assert.AreEqual(63, new ScanOptions { MaxParallelPartitions = 8, PrefetchPages = 64 }.ForJob(null).PrefetchPages, "8 x 64 pages fit");
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { MaxParallelPartitions = 100, PrefetchPages = 10 }));
        Assert.IsNull(ScanOptions.Validate(new JobPerformanceSettings { MaxParallelPartitions = 100, PrefetchPages = 4 }));
    }

    [TestMethod]
    public void ScanSettings_AboveTheLimit_AreClamped_WithAWarning()
    {
        var warnings = new List<string>();
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["S3Compare:MaxListRequestsPerSecond"] = "250000",
            ["S3Compare:MaxParallelPartitions"] = "1000",
            ["S3Compare:PrefetchPages"] = "-3",
            ["S3Compare:MaxObjectsPerSecond"] = "abc"
        }).Build();
        var scan = HardeningSettings.Scan(config, warnings.Add);

        Assert.AreEqual(ScanOptions.MaxListRequestsPerSecondLimit, scan.MaxListRequestsPerSecond);
        Assert.AreEqual(ScanOptions.MaxParallelPartitionsLimit, scan.MaxParallelPartitions);
        Assert.AreEqual(1, scan.PrefetchPages, "default 2 lowered by the memory cap for 256 partitions");
        Assert.AreEqual(ScanOptions.DefaultMaxObjectsPerSecond, scan.MaxObjectsPerSecond);
        Assert.IsTrue(warnings.Any(w => w.Contains("MaxListRequestsPerSecond") && w.Contains("100000")), string.Join(" | ", warnings));
        Assert.IsTrue(warnings.Any(w => w.Contains("PrefetchPages = '-3'")));
        Assert.IsTrue(warnings.Any(w => w.Contains("MaxObjectsPerSecond")));
        Assert.IsTrue(warnings.Any(w => w.Contains("lowered")));
    }

    // ---------------- 10. corrupt job settings ----------------

    [TestMethod]
    public async Task CorruptJobJson_FallsBackToDefaults_AndIsLogged()
    {
        var repo = new JobRepository(Factory, AuditRepo);
        var job = new ComparisonJob { Name = "corrupt" };
        job.Performance.MaxParallelPartitions = 4;
        job.AlertConfig.MaxDisparitiesThreshold = 7;
        await repo.SaveAsync(job);
        await repo.SaveAsync(new ComparisonJob { Name = "fine" });
        await using (var conn = await Factory.CreateOpenConnectionAsync())
        {
            await Dapper.SqlMapper.ExecuteAsync(conn, "UPDATE Jobs SET Performance_Json = '{not json', AlertConfig_Json = '[1,' WHERE Id = @Id", new { job.Id });
        }

        var all = await repo.GetAllAsync();
        Assert.AreEqual(2, all.Count, "one corrupt row does not break the list");
        var loaded = all.Single(j => j.Id == job.Id);
        Assert.IsTrue(loaded.Performance.IsEmpty);
        Assert.AreEqual(0, loaded.AlertConfig.MaxDisparitiesThreshold);

        List<ServiceAuditLog> logs = new();
        for (int i = 0; i < 100 && logs.Count(l => l.Message.Contains("could not be read")) < 2; i++)
        {
            await Task.Delay(20);
            logs = await AuditRepo.GetRecentLogsAsync(50);
        }
        Assert.IsTrue(logs.Any(l => l.Message.Contains("Performance_Json")), string.Join(" | ", logs.Select(l => l.Message)));
        Assert.IsTrue(logs.Any(l => l.Message.Contains("AlertConfig_Json")));
    }

    // ---------------- 12. events are raised without an engine lock ----------------

    [TestMethod]
    public async Task SlowDifferenceHandler_DoesNotBlockOtherWorkers()
    {
        var keys = Enumerable.Range(0, 20).SelectMany(d => Enumerable.Range(0, 30).Select(f => $"d{d:00}/f{f:00}")).ToList();
        var a = keys.Select(k => Obj(k)).ToList();
        var b = keys.Where((_, i) => i % 3 != 0).Select(k => Obj(k)).ToList(); // 200 differences spread over all ranges
        var job = NewJob();
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(4, pageSize: 10), listerA: new FakeS3Lister(a), listerB: new FakeS3Lister(b));
        int raised = 0, raisedWhileBlocked = -1;
        engine.DifferenceFound += _ =>
        {
            if (Interlocked.Increment(ref raised) == 1)
            {
                Thread.Sleep(400);
                raisedWhileBlocked = Volatile.Read(ref raised) - 1;
            }
        };

        var finished = await engine.RunAsync();
        Assert.AreEqual(200, finished.TotalDifferences);
        Assert.AreEqual(200, raised);
        Assert.IsTrue(raisedWhileBlocked > 0, "other workers kept raising events while one handler was slow");
    }

    // ---------------- 7. the planner honours pause ----------------

    [TestMethod]
    public async Task Planner_WaitsWhilePaused()
    {
        var keys = Enumerable.Range(0, 500).Select(i => $"file_{i:00000}.dat").Select(k => Obj(k)).ToList();
        var listerA = new FakeS3Lister(keys);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var planner = new PartitionPlanner(listerA, "a", null, null, new FakeS3Lister(keys), "b", null, null, 8,
            pauseGate: ct => gate.Task.WaitAsync(ct));

        var plan = planner.PlanAsync(CancellationToken.None);
        await Task.Delay(150);
        Assert.IsFalse(plan.IsCompleted);
        Assert.AreEqual(0, listerA.Calls, "no request while paused");

        gate.SetResult();
        var ranges = await plan;
        Assert.IsTrue(listerA.Calls > 0);
        Assert.IsTrue(ranges.Count > 1);
    }

    [TestMethod]
    public async Task PausedEngine_DoesNotPlan()
    {
        var keys = Enumerable.Range(0, 2000).Select(i => $"file_{i:00000}.dat").Select(k => Obj(k)).ToList();
        var listerA = new FakeS3Lister(keys);
        var job = NewJob();
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(4, pageSize: 100), listerA: listerA, listerB: new FakeS3Lister(keys));
        engine.Pause();
        var task = engine.RunAsync();
        await Task.Delay(200);
        Assert.AreEqual(0, listerA.Calls, "neither the planner nor the listings run while paused");
        engine.Resume();
        Assert.AreEqual(2000, (await task).MatchedCount);
    }
}
