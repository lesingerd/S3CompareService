using System.Reflection;
using Amazon.S3;
using Amazon.S3.Model;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

[TestClass]
public class EngineResilienceTests : TestDbBase
{
    private static S3ObjectSummary Obj(string key, long size = 10, string etag = "e")
        => new() { Key = key, Size = size, ETag = etag, LastModified = DateTime.UtcNow.AddDays(-2) };

    private static ComparisonJob NewJob(string name = "resilience")
        => new() { Name = name, ReplicationGracePeriodMinutes = 0, DetectSequenceGaps = false };

    private async Task<ScanRun> NewRunAsync(ComparisonJob job)
    {
        var run = new ScanRun { JobId = job.Id, JobName = job.Name };
        await RunRepo.CreateAsync(run);
        return run;
    }

    // ---------- 1 / 5: real mid-scan cancel -> resume into the same run ----------

    [TestMethod]
    public async Task CancelMidScan_ThenResumeIntoSameRun_NoKeysSkippedOrDuplicated()
    {
        var itemsA = Enumerable.Range(0, 40).Select(i => Obj($"k{i:000}")).ToList();
        var missingInB = new HashSet<string> { "k005", "k012", "k030" };
        var itemsB = itemsA.Where(o => !missingInB.Contains(o.Key)).Append(Obj("k100")).ToList(); // k100 only in B

        var job = NewJob();
        var run = await NewRunAsync(job);
        using var cts = new CancellationTokenSource();
        ServiceComparisonEngine? engine1 = null;

        // Stream A cancels the scan (as a service shutdown) when it hands out its 15th item.
        var streamA = new MockS3ObjectStream(itemsA, onMoveNext: n =>
        {
            if (n == 15) { engine1!.MarkServiceShutdown(); cts.Cancel(); }
        });
        engine1 = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, streamA: streamA, streamB: new MockS3ObjectStream(itemsB));

        await Assert.ThrowsAsync<OperationCanceledException>(() => engine1.RunAsync(cts.Token));

        var interrupted = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(RunStatus.Interrupted, interrupted.Status);
        Assert.IsNotNull(interrupted.EndTimeUtc);
        var cp = await RunRepo.GetCheckpointAsync(run.Id);
        Assert.IsNotNull(cp, "checkpoint is written on interruption");
        Assert.IsTrue(cp.HasKey);
        // 15 items were pulled from A, but the 15th was only look-ahead, so the last fully processed key is k013.
        Assert.AreEqual("k013", cp.LastEvaluatedKey);
        Assert.AreEqual(14, cp.ObjectsCompared);
        Assert.AreEqual(14, cp.BucketAObjectsRead, "look-ahead item must not be counted");

        // Recovery: the run shows up as unfinished, is re-opened (same id) and resumed from its checkpoint.
        Assert.AreEqual(run.Id, (await RunRepo.GetUnfinishedRunsAsync()).Single().Id);
        var reopened = (await RunRepo.ReopenForResumeAsync(run.Id))!;
        Assert.AreEqual(run.Id, reopened.Id);

        var engine2 = new ServiceComparisonEngine(job, reopened, RunRepo, DiffRepo, AuditRepo, cp,
            new MockS3ObjectStream(itemsA, startAfter: cp.LastEvaluatedKey),
            new MockS3ObjectStream(itemsB, startAfter: cp.LastEvaluatedKey));
        Assert.IsTrue(engine2.IsResuming);
        var finished = await engine2.RunAsync();

        Assert.AreEqual(RunStatus.Completed, finished.Status);
        Assert.AreEqual(run.Id, finished.Id);
        Assert.AreEqual(41, finished.ObjectsCompared); // 40 in A + k100 only in B
        Assert.AreEqual(37, finished.MatchedCount);
        Assert.AreEqual(4, finished.TotalDifferences);
        Assert.AreEqual(40, finished.BucketAObjectsRead);
        Assert.AreEqual(38, finished.BucketBObjectsRead);

        // All differences live under the one run id, each exactly once, and match the run statistics.
        var stored = await DiffRepo.GetAllForRunAsync(run.Id);
        CollectionAssert.AreEquivalent(new[] { "k005", "k012", "k030", "k100" }, stored.Select(d => d.Key).ToList());
        Assert.AreEqual(finished.TotalDifferences, stored.Count);
        Assert.AreEqual(1, stored.Count(d => d.Type == DifferenceType.MissingInBucketA));

        var final = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(RunStatus.Completed, final.Status);
        Assert.AreEqual(1, final.ResumeCount);
        Assert.IsTrue(final.IsResumedFromCheckpoint);
        Assert.AreEqual("k013", final.ResumedFromKey);
        Assert.IsNull(await RunRepo.GetCheckpointAsync(run.Id), "checkpoint is deleted after completion");
        Assert.AreEqual(0, (await RunRepo.GetUnfinishedRunsAsync()).Count);
    }

    [TestMethod]
    public async Task LegacyResumeWithNewRunId_AdoptsOldDifferences_AndSupersedesOldRun()
    {
        var itemsA = Enumerable.Range(0, 30).Select(i => Obj($"f{i:000}")).ToList();
        var itemsB = itemsA.Where(o => o.Key != "f003" && o.Key != "f025").ToList(); // 2 missing in B

        var job = NewJob();
        var run1 = await NewRunAsync(job);
        using var cts = new CancellationTokenSource();
        ServiceComparisonEngine? engine1 = null;
        engine1 = new ServiceComparisonEngine(job, run1, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(itemsA, onMoveNext: n => { if (n == 10) { engine1!.MarkServiceShutdown(); cts.Cancel(); } }),
            streamB: new MockS3ObjectStream(itemsB));
        await Assert.ThrowsAsync<OperationCanceledException>(() => engine1.RunAsync(cts.Token));
        var cp = (await RunRepo.GetCheckpointAsync(run1.Id))!;

        // The current Worker flow: brand-new run id + the old checkpoint.
        var run2 = await NewRunAsync(job);
        var engine2 = new ServiceComparisonEngine(job, run2, RunRepo, DiffRepo, AuditRepo, cp,
            new MockS3ObjectStream(itemsA, startAfter: cp.LastEvaluatedKey),
            new MockS3ObjectStream(itemsB, startAfter: cp.LastEvaluatedKey));
        var finished = await engine2.RunAsync();

        Assert.AreEqual(2, finished.TotalDifferences);
        var stored = await DiffRepo.GetAllForRunAsync(run2.Id);
        CollectionAssert.AreEquivalent(new[] { "f003", "f025" }, stored.Select(d => d.Key).ToList());
        Assert.AreEqual(0, (await DiffRepo.GetAllForRunAsync(run1.Id)).Count, "differences moved to the new run");
        Assert.AreEqual(run2.Id, (await RunRepo.GetByIdAsync(run1.Id))!.SupersededByRunId);
        Assert.IsNull(await RunRepo.GetCheckpointAsync(run1.Id));
        Assert.IsFalse((await RunRepo.GetUnfinishedRunsAsync()).Any(r => r.Id == run1.Id));
    }

    [TestMethod]
    public async Task MismatchedCheckpoint_IsDiscarded_AndScanStartsFresh()
    {
        var itemsA = new[] { Obj("a"), Obj("b"), Obj("c") }.ToList();
        var job = NewJob();
        job.BucketA.Prefix = "new-prefix/";
        var run = await NewRunAsync(job);

        var stale = new ScanCheckpoint
        {
            ScanRunId = run.Id, JobId = job.Id, LastEvaluatedKey = "b", HasLastEvaluatedKey = true, ObjectsCompared = 2,
            BucketNameA = job.BucketA.BucketName, EndpointA = job.BucketA.EndpointUrl, PrefixA = "old-prefix/",
            BucketNameB = job.BucketB.BucketName, EndpointB = job.BucketB.EndpointUrl, PrefixB = "", Mode = job.Mode
        };
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, stale,
            new MockS3ObjectStream(itemsA), new MockS3ObjectStream(itemsA));

        Assert.IsFalse(engine.IsResuming);
        StringAssert.Contains(engine.CheckpointDiscardReason, "prefix");
        var finished = await engine.RunAsync();
        Assert.AreEqual(3, finished.ObjectsCompared, "counters were NOT seeded from the stale checkpoint");
        Assert.AreEqual(3, finished.MatchedCount);
    }

    // ---------- 2: flush failure must not advance the checkpoint ----------

    private sealed class FailingDiffRepo : IDifferenceRepository
    {
        private readonly IDifferenceRepository _inner;
        public int FailuresRemaining;
        public int InsertAttempts;
        public FailingDiffRepo(IDifferenceRepository inner, int failures) { _inner = inner; FailuresRemaining = failures; }

        public Task InsertBatchAsync(IEnumerable<RecordedDifference> differences, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref InsertAttempts);
            if (FailuresRemaining != 0)
            {
                if (FailuresRemaining > 0) FailuresRemaining--;
                throw new InvalidOperationException("disk full (simulated)");
            }
            return _inner.InsertBatchAsync(differences, cancellationToken);
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
    public async Task FailedFlush_FailsScan_WritesFinalState_AndDoesNotAdvanceCheckpoint()
    {
        var itemsA = Enumerable.Range(0, 20).Select(i => Obj($"m{i:000}")).ToList();
        var job = NewJob();
        var run = await NewRunAsync(job);
        var failing = new FailingDiffRepo(DiffRepo, failures: -1); // always fails

        var engine = new ServiceComparisonEngine(job, run, RunRepo, failing, AuditRepo,
            streamA: new MockS3ObjectStream(itemsA), streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));

        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.RunAsync());

        var r = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(RunStatus.Failed, r.Status, "CompleteAsync still ran");
        Assert.IsNotNull(r.EndTimeUtc);
        StringAssert.Contains(r.ErrorMessage, "could not be persisted");
        Assert.IsNull(await RunRepo.GetCheckpointAsync(run.Id), "no checkpoint may claim keys whose differences were lost");
        Assert.AreEqual(0, (await DiffRepo.GetAllForRunAsync(run.Id)).Count);
    }

    [TestMethod]
    public async Task TransientFlushFailure_IsRetried_WithoutLosingTheBatch()
    {
        var itemsA = Enumerable.Range(0, 5).Select(i => Obj($"t{i}")).ToList();
        var job = NewJob();
        var run = await NewRunAsync(job);
        var flaky = new FailingDiffRepo(DiffRepo, failures: 2); // succeeds on the 3rd attempt

        var engine = new ServiceComparisonEngine(job, run, RunRepo, flaky, AuditRepo,
            streamA: new MockS3ObjectStream(itemsA), streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));
        var finished = await engine.RunAsync();

        Assert.AreEqual(RunStatus.Completed, finished.Status);
        Assert.AreEqual(5, (await DiffRepo.GetAllForRunAsync(run.Id)).Count);
        Assert.AreEqual(3, flaky.InsertAttempts);
    }

    // ---------- >500 item batch flush (+ resume across flushed batches) ----------

    [TestMethod]
    public async Task LargeScan_FlushesInBatches_AndResumeAcrossFlushedBatchesHasNoDuplicates()
    {
        var itemsA = Enumerable.Range(0, 1300).Select(i => Obj($"b{i:0000}")).ToList(); // all missing in B
        var job = NewJob();
        var run = await NewRunAsync(job);
        var counting = new FailingDiffRepo(DiffRepo, failures: 0);
        using var cts = new CancellationTokenSource();
        ServiceComparisonEngine? engine1 = null;

        engine1 = new ServiceComparisonEngine(job, run, RunRepo, counting, AuditRepo,
            streamA: new MockS3ObjectStream(itemsA, onMoveNext: n => { if (n == 1100) { engine1!.MarkServiceShutdown(); cts.Cancel(); } }),
            streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));
        await Assert.ThrowsAsync<OperationCanceledException>(() => engine1.RunAsync(cts.Token));

        Assert.IsTrue(counting.InsertAttempts >= 2, "500-item batches were flushed before the interruption");
        var cp = (await RunRepo.GetCheckpointAsync(run.Id))!;
        Assert.AreEqual(1099, cp.ObjectsCompared);
        Assert.AreEqual(1099, await DiffRepo.GetDifferenceCountAsync(run.Id), "stored differences == checkpointed count");

        var reopened = (await RunRepo.ReopenForResumeAsync(run.Id))!;
        var engine2 = new ServiceComparisonEngine(job, reopened, RunRepo, DiffRepo, AuditRepo, cp,
            new MockS3ObjectStream(itemsA, startAfter: cp.LastEvaluatedKey), new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));
        var finished = await engine2.RunAsync();

        var keys = (await DiffRepo.GetAllForRunAsync(run.Id)).Select(d => d.Key).ToList();
        Assert.AreEqual(1300, keys.Count);
        Assert.AreEqual(1300, keys.Distinct().Count());
        Assert.AreEqual(1300, finished.MissingInBucketBCount);
    }

    [TestMethod]
    public async Task Resume_DropsDifferencesFlushedAfterTheLastCheckpoint()
    {
        // Simulates a hard crash: differences past the checkpoint key made it to the database (batch flush) but the
        // checkpoint is older. Resuming must not duplicate them.
        var itemsA = Enumerable.Range(0, 10).Select(i => Obj($"c{i}")).ToList(); // all missing in B
        var job = NewJob();
        var run = await NewRunAsync(job);
        await RunRepo.UpdateProgressAsync(run);

        await DiffRepo.InsertBatchAsync(Enumerable.Range(0, 7).Select(i =>
            new RecordedDifference { ScanRunId = run.Id, Key = $"c{i}", Type = DifferenceType.MissingInBucketB }));
        var cp = new ScanCheckpoint
        {
            ScanRunId = run.Id, JobId = job.Id, LastEvaluatedKey = "c3", HasLastEvaluatedKey = true,
            ObjectsCompared = 4, TotalDifferences = 4, FilteredDifferencesCount = 4, MissingInBucketBCount = 4,
            BucketAObjectsRead = 4, Mode = job.Mode, BucketNameA = job.BucketA.BucketName, EndpointA = job.BucketA.EndpointUrl,
            BucketNameB = job.BucketB.BucketName, EndpointB = job.BucketB.EndpointUrl
        };

        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, cp,
            new MockS3ObjectStream(itemsA, startAfter: "c3"), new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));
        var finished = await engine.RunAsync();

        Assert.AreEqual(10, finished.TotalDifferences);
        var keys = (await DiffRepo.GetAllForRunAsync(run.Id)).Select(d => d.Key).ToList();
        Assert.AreEqual(10, keys.Count);
        Assert.AreEqual(10, keys.Distinct().Count());
    }

    // ---------- 9: terminal state / cancellation semantics ----------

    [TestMethod]
    public async Task UserCancel_ProducesCancelled_NotInterrupted()
    {
        var itemsA = Enumerable.Range(0, 20).Select(i => Obj($"u{i:00}")).ToList();
        var job = NewJob();
        var run = await NewRunAsync(job);
        using var cts = new CancellationTokenSource();
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(itemsA, onMoveNext: n => { if (n == 5) cts.Cancel(); }),
            streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));

        await Assert.ThrowsAsync<OperationCanceledException>(() => engine.RunAsync(cts.Token));
        var r = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(RunStatus.Cancelled, r.Status);
        Assert.AreEqual(0, (await RunRepo.GetUnfinishedRunsAsync()).Count);
    }

    private sealed class TimeoutStream : IS3ObjectStream
    {
        public S3ObjectSummary? Current => null;
        public long TotalObjectsRead => 0;
        public long TotalBytesRead => 0;
        public DateTime? OldestLastModifiedUtc => null;
        public DateTime? NewestLastModifiedUtc => null;
        public IReadOnlyDictionary<string, long> StorageClassCounts { get; } = new Dictionary<string, long>();
        public bool HasMore => true;
        public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default) => throw new TaskCanceledException("HTTP timeout (not a user cancel)");
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [TestMethod]
    public async Task InternalTimeout_WithoutCallerCancellation_IsFailed()
    {
        var job = NewJob();
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, streamA: new TimeoutStream(), streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));

        await Assert.ThrowsAsync<TaskCanceledException>(() => engine.RunAsync());
        Assert.AreEqual(RunStatus.Failed, (await RunRepo.GetByIdAsync(run.Id))!.Status);
    }

    [TestMethod]
    public async Task ThrowingEventHandlers_DoNotFailTheScan()
    {
        var itemsA = new[] { Obj("x1"), Obj("x2") }.ToList();
        var job = NewJob();
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(itemsA), streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));
        engine.DifferenceFound += _ => throw new InvalidOperationException("subscriber bug");
        engine.ProgressUpdated += _ => throw new InvalidOperationException("subscriber bug");

        var finished = await engine.RunAsync();
        Assert.AreEqual(RunStatus.Completed, finished.Status);
        Assert.AreEqual(2, (await DiffRepo.GetAllForRunAsync(run.Id)).Count);
    }

    [TestMethod]
    public async Task PauseResume_StatusWritesAreOrdered_AndFinalStateWins()
    {
        var itemsA = Enumerable.Range(0, 50).Select(i => Obj($"p{i:00}")).ToList();
        var job = NewJob();
        var run = await NewRunAsync(job);
        ServiceComparisonEngine? engine = null;
        engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(itemsA, onMoveNext: n =>
            {
                if (n == 10) engine!.Pause();
            }),
            streamB: new MockS3ObjectStream(itemsA));

        var task = engine.RunAsync();
        for (int i = 0; i < 100 && !engine.IsPaused; i++) await Task.Delay(10);
        await Task.Delay(100);
        Assert.IsFalse(task.IsCompleted, "paused engine must not finish");
        for (int i = 0; i < 300 && (await RunRepo.GetByIdAsync(run.Id))!.Status != RunStatus.Paused; i++) await Task.Delay(10);
        Assert.AreEqual(RunStatus.Paused, (await RunRepo.GetByIdAsync(run.Id))!.Status);

        engine.Resume();
        var finished = await task;
        Assert.AreEqual(RunStatus.Completed, finished.Status);
        Assert.AreEqual(RunStatus.Completed, (await RunRepo.GetByIdAsync(run.Id))!.Status);
    }

    // ---------- 7: merge ordering uses UTF-8 byte order ----------

    [TestMethod]
    public async Task Merge_UsesUtf8Order_ForSupplementaryVsHighBmpKeys()
    {
        string emoji = "dir/\U0001F600.txt";
        string fullwidth = "dir/！.txt";
        var itemsA = new[] { Obj(fullwidth), Obj(emoji) }.ToList();
        var itemsB = new[] { Obj(emoji) }.ToList();

        var job = NewJob();
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, streamA: new MockS3ObjectStream(itemsA), streamB: new MockS3ObjectStream(itemsB));
        var finished = await engine.RunAsync();

        // Correct UTF-8 ordering: fullwidth < emoji, so fullwidth is simply missing in B and the emoji matches.
        Assert.AreEqual(1, finished.TotalDifferences);
        Assert.AreEqual(1, finished.MatchedCount);
        Assert.AreEqual(fullwidth, (await DiffRepo.GetAllForRunAsync(run.Id)).Single().Key);
    }

    // ---------- 8: start-after key construction ----------

    [TestMethod]
    public void BuildStartAfterKey_AlwaysPrefixPlusRelativeKey()
    {
        Assert.IsNull(ServiceComparisonEngine.BuildStartAfterKey(false, "", "p/"));
        Assert.AreEqual("p/abc", ServiceComparisonEngine.BuildStartAfterKey(true, "abc", "p/"));
        // A relative key that happens to start with the prefix text must still get the prefix prepended.
        Assert.AreEqual("p/p/x", ServiceComparisonEngine.BuildStartAfterKey(true, "p/x", "p/"));
        Assert.AreEqual("p/", ServiceComparisonEngine.BuildStartAfterKey(true, "", "p/"), "empty relative key (object == prefix) is a valid checkpoint");
        Assert.AreEqual(" lead", ServiceComparisonEngine.BuildStartAfterKey(true, " lead", null), "no Trim");
    }

    [TestMethod]
    public async Task Checkpoint_IsWrittenEvenWhenRelativeKeyIsEmpty()
    {
        // An object whose key equals the bucket prefix has an empty relative key.
        var job = NewJob();
        job.BucketA.Prefix = "p/";
        job.BucketB.Prefix = "p/";
        var run = await NewRunAsync(job);
        using var cts = new CancellationTokenSource();
        var itemsA = new[] { Obj("p/"), Obj("p/a"), Obj("p/b") }.ToList();
        ServiceComparisonEngine? engine = null;
        engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(itemsA, onMoveNext: n => { if (n == 2) { engine!.MarkServiceShutdown(); cts.Cancel(); } }),
            streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));
        await Assert.ThrowsAsync<OperationCanceledException>(() => engine.RunAsync(cts.Token));

        var cp = await RunRepo.GetCheckpointAsync(run.Id);
        Assert.IsNotNull(cp);
        Assert.AreEqual(string.Empty, cp.LastEvaluatedKey);
        Assert.IsTrue(cp.HasKey);
    }

    // ---------- 18 / 19: paging enumerator ----------

    public class FakeS3Proxy : DispatchProxy
    {
        public List<ListObjectsV2Response> Pages = new();
        public int Calls;
        public List<ListObjectsV2Request> Requests = new();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == "ListObjectsV2Async")
            {
                Requests.Add((ListObjectsV2Request)args![0]!);
                var page = Pages[Math.Min(Calls++, Pages.Count - 1)];
                return Task.FromResult(page);
            }
            if (targetMethod.Name == "Dispose") return null;
            throw new NotSupportedException(targetMethod.Name);
        }
    }

    private static ListObjectsV2Response Page(bool truncated, string? token, params string[] keys)
        => new()
        {
            IsTruncated = truncated,
            NextContinuationToken = token,
            S3Objects = keys.Select(k => new S3Object { Key = k, Size = 100, ETag = "\"e\"", LastModified = DateTime.UtcNow, StorageClass = S3StorageClass.Standard }).ToList()
        };

    [TestMethod]
    public async Task PagingEnumerator_CountsOnConsumption_NotOnPageLoad()
    {
        var proxy = DispatchProxy.Create<IAmazonS3, FakeS3Proxy>();
        ((FakeS3Proxy)(object)proxy).Pages = new() { Page(false, null, "a", "b", "c") };

        await using var en = new S3PagingEnumerator(proxy, "bucket", "", initialObjectsRead: 10, initialBytesRead: 1000);
        Assert.IsTrue(await en.MoveNextAsync());
        Assert.AreEqual(11, en.TotalObjectsRead, "one consumed, two merely prefetched");
        Assert.AreEqual(1100, en.TotalBytesRead);
        Assert.AreEqual(1, en.StorageClassCounts.Values.Sum());
        Assert.IsTrue(await en.MoveNextAsync());
        Assert.IsTrue(await en.MoveNextAsync());
        Assert.IsFalse(await en.MoveNextAsync());
        Assert.AreEqual(13, en.TotalObjectsRead);
    }

    [TestMethod]
    public async Task PagingEnumerator_TruncatedWithoutToken_ThrowsInsteadOfLooping()
    {
        var proxy = DispatchProxy.Create<IAmazonS3, FakeS3Proxy>();
        var fake = (FakeS3Proxy)(object)proxy;
        fake.Pages = new() { Page(true, null, "a") };

        await using var en = new S3PagingEnumerator(proxy, "bucket", "");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => en.MoveNextAsync());
        StringAssert.Contains(ex.Message, "NextContinuationToken");
        Assert.AreEqual(1, fake.Calls);
    }

    [TestMethod]
    public async Task PagingEnumerator_DoesNotTrimPrefixOrStartAfter()
    {
        var proxy = DispatchProxy.Create<IAmazonS3, FakeS3Proxy>();
        var fake = (FakeS3Proxy)(object)proxy;
        fake.Pages = new() { Page(false, null) };

        await using var en = new S3PagingEnumerator(proxy, "bucket", " p/ ", startAfter: " p/ k ");
        await en.MoveNextAsync();
        Assert.AreEqual(" p/ ", fake.Requests[0].Prefix);
        Assert.AreEqual(" p/ k ", fake.Requests[0].StartAfter);
    }

    // ---------- 10: sequence gaps ----------

    [TestMethod]
    public void SequenceGaps_UnpaddedNames_DoNotProduceFalseGaps()
    {
        var detector = new SequenceGapDetector(25);
        long id = 0;
        // S3 lexicographic order of unpadded names: file1, file10, file100, ..., file19, file2, file20 ... (complete series, nothing missing)
        var keys = Enumerable.Range(1, 120).Select(i => $"file{i}").OrderBy(k => k, StringComparer.Ordinal).ToArray();
        int gaps = keys.Sum(k => detector.CheckKey(k, "run", ref id).Count);
        Assert.AreEqual(0, gaps);
    }

    [TestMethod]
    public void SequenceGaps_PaddedNames_StillDetectRealGaps()
    {
        var detector = new SequenceGapDetector(25);
        long id = 0;
        var found = new List<string>();
        foreach (var k in new[] { "img_0001.jpg", "img_0002.jpg", "img_0005.jpg", "img_0006.jpg" })
            found.AddRange(detector.CheckKey(k, "run", ref id).Select(d => d.Key));
        CollectionAssert.AreEqual(new[] { "img_0003.jpg", "img_0004.jpg" }, found);
    }
}
