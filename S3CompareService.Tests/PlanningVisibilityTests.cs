using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Worker;
using S3CompareService.Worker.Services;

namespace S3CompareService.Tests;

/// <summary>A lister that can throw, hang, or block a thread for chosen requests.</summary>
internal sealed class MisbehavingLister : IS3Lister
{
    private readonly IS3Lister _inner;
    public Func<int, string?, string?, string?, int, Task>? Behaviour { get; set; }
    private int _calls;
    public MisbehavingLister(IS3Lister inner) { _inner = inner; }
    public int Calls => Volatile.Read(ref _calls);

    public async Task<S3ListPage> ListPageAsync(string bucket, string? prefix, string? startAfter, string? continuationToken, string? delimiter, int maxKeys, CancellationToken cancellationToken)
    {
        int call = Interlocked.Increment(ref _calls);
        if (Behaviour != null) await Behaviour(call, prefix, startAfter, delimiter, maxKeys).ConfigureAwait(false);
        return await _inner.ListPageAsync(bucket, prefix, startAfter, continuationToken, delimiter, maxKeys, cancellationToken).ConfigureAwait(false);
    }
}

[TestClass]
public class PlannerRobustnessTests
{
    private static S3ObjectSummary Obj(string key) => new() { Key = key, Size = 1, ETag = "e", LastModified = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), StorageClass = "STANDARD" };
    private static List<string> FlatKeys(int n) => Enumerable.Range(0, n).Select(i => $"file_{i:00000}.dat").ToList();
    private static FakeS3Lister Fake(IEnumerable<string> keys, TimeSpan? latency = null) => new(keys.Select(Obj), latency);

    private sealed class Logs
    {
        public ConcurrentQueue<(string Level, string Message)> Items { get; } = new();
        public void Add(string level, string message) => Items.Enqueue((level, message));
        public int Count(string level, string contains) => Items.Count(i => i.Level == level && i.Message.Contains(contains, StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task TimeBox_StopsPlanning_AndBuildsValidRangesFromPartialCandidates()
    {
        var keys = FlatKeys(5000);
        var a = Fake(keys, TimeSpan.FromMilliseconds(60));
        var b = Fake(keys, TimeSpan.FromMilliseconds(60));
        var logs = new Logs();
        var planner = new PartitionPlanner(a, "a", null, null, b, "b", null, null, 64, 3, 100_000, null,
            maxPlanningTime: TimeSpan.FromMilliseconds(500), log: logs.Add);

        var sw = Stopwatch.StartNew();
        var ranges = await planner.PlanAsync(CancellationToken.None);
        sw.Stop();

        Assert.IsTrue(planner.TimedOut, "the time limit should have ended planning");
        Assert.IsTrue(sw.Elapsed < TimeSpan.FromSeconds(5), $"planning took {sw.Elapsed}");
        Assert.IsNull(PartitionPlanner.ValidateTiling(ranges), string.Join(" ", ranges));
        Assert.IsTrue(ranges.Count >= 2, $"partial candidates should still split the keyspace, got {ranges.Count} range(s)");
        Assert.IsTrue(ranges.Count < 64, "it must not have finished the whole plan");
        Assert.AreEqual(1, logs.Count("WARN", "planning time limit"), "the time limit is logged once");
        Assert.AreEqual(0, planner.RequestErrors, "a request cut short by the time limit is not an error");
    }

    [TestMethod]
    public async Task MaxCodePointStartAfter_Failing_DisablesTheJump_AndPlanningStillSplits()
    {
        var keys = FlatKeys(3000);
        var bad = new MisbehavingLister(Fake(keys))
        {
            Behaviour = (_, _, startAfter, _, _) => startAfter != null && startAfter.EndsWith("\U0010FFFF", StringComparison.Ordinal)
                ? throw new InvalidOperationException("InvalidArgument: key too long (simulated)")
                : Task.CompletedTask
        };
        var logs = new Logs();
        var planner = new PartitionPlanner(bad, "a", null, null, Fake(keys), "b", null, null, 32, 3, 500, null, log: logs.Add);

        var ranges = await planner.PlanAsync(CancellationToken.None);

        Assert.AreEqual("~ fallback", planner.JumpMode);
        Assert.AreEqual(1, logs.Count("WARN", "falling back"), "the marker switch is logged once");
        Assert.IsTrue(planner.RequestErrors >= 1);
        Assert.IsNull(PartitionPlanner.ValidateTiling(ranges));
        Assert.IsTrue(ranges.Count > 4, $"probes must still produce a multi-range plan, got {ranges.Count}");
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task HungRequest_HitsThePerRequestTimeout_AndPlanningContinues(bool honoursCancellation)
    {
        var keys = FlatKeys(3000);
        var hang = new MisbehavingLister(Fake(keys));
        hang.Behaviour = (call, _, _, _, _) => call == 2 ? new TaskCompletionSource().Task : Task.CompletedTask; // never completes, ignores the token
        _ = honoursCancellation; // the fake ignores the token in both variants: the timeout must not depend on the lister
        var logs = new Logs();
        var planner = new PartitionPlanner(hang, "a", null, null, Fake(keys), "b", null, null, 32, 3, 500, null,
            requestTimeout: TimeSpan.FromMilliseconds(300), log: logs.Add);

        var sw = Stopwatch.StartNew();
        var ranges = await planner.PlanAsync(CancellationToken.None);

        Assert.IsTrue(sw.Elapsed < TimeSpan.FromSeconds(10), $"took {sw.Elapsed}");
        Assert.IsTrue(planner.RequestErrors >= 1);
        Assert.IsTrue(logs.Count("WARN", "timed out after") >= 1, "the timed-out request is logged as a warning");
        Assert.IsNull(PartitionPlanner.ValidateTiling(ranges));
        Assert.IsTrue(ranges.Count > 2);
    }

    [TestMethod]
    public async Task EveryStartAfterJumpFailing_DisablesTheJump_AndPlanningStillReturnsValidRanges()
    {
        var keys = FlatKeys(3000);
        var bad = new MisbehavingLister(Fake(keys))
        {
            Behaviour = (_, _, startAfter, _, maxKeys) => startAfter != null && maxKeys == 1 && (startAfter.EndsWith("\U0010FFFF", StringComparison.Ordinal) || startAfter.EndsWith("~", StringComparison.Ordinal))
                ? throw new InvalidOperationException("rejected (simulated)")
                : Task.CompletedTask
        };
        var logs = new Logs();
        var planner = new PartitionPlanner(bad, "a", null, null, Fake(keys), "b", null, null, 32, 3, 500, null, log: logs.Add);
        var ranges = await planner.PlanAsync(CancellationToken.None);
        Assert.IsTrue(planner.JumpDisabled);
        Assert.AreEqual("disabled", planner.JumpMode);
        Assert.AreEqual(1, logs.Count("WARN", "jump is disabled"));
        Assert.IsNull(PartitionPlanner.ValidateTiling(ranges));
    }

    [TestMethod]
    public async Task FolderStructure_StillSplits_WhenTheJumpMarkerIsRejected()
    {
        var keys = Enumerable.Range(0, 40).SelectMany(d => Enumerable.Range(0, 5).Select(f => $"dir{d:00}/f{f}")).ToList();
        var bad = new MisbehavingLister(Fake(keys))
        {
            Behaviour = (_, _, startAfter, _, _) => startAfter != null && startAfter.EndsWith("\U0010FFFF", StringComparison.Ordinal) ? throw new InvalidOperationException("rejected") : Task.CompletedTask
        };
        var planner = new PartitionPlanner(bad, "a", null, null, Fake(keys), "b", null, null, 16, 3, 500);
        var ranges = await planner.PlanAsync(CancellationToken.None);
        Assert.IsTrue(ranges.Count > 8);
        Assert.IsNull(PartitionPlanner.ValidateTiling(ranges));
    }

    [TestMethod]
    public async Task ScanCancellation_StillPropagates()
    {
        var keys = FlatKeys(3000);
        using var cts = new CancellationTokenSource();
        var slow = Fake(keys, TimeSpan.FromMilliseconds(200));
        var planner = new PartitionPlanner(slow, "a", null, null, Fake(keys, TimeSpan.FromMilliseconds(200)), "b", null, null, 32, 3, 500);
        cts.CancelAfter(150);
        await Assert.ThrowsAsync<OperationCanceledException>(() => planner.PlanAsync(cts.Token));
    }

    [TestMethod]
    public async Task IndependentRequests_RunConcurrently_UpToTheCap()
    {
        var keys = FlatKeys(20000);
        var a = Fake(keys, TimeSpan.FromMilliseconds(25));
        var b = Fake(keys, TimeSpan.FromMilliseconds(25));
        var planner = new PartitionPlanner(a, "a", null, null, b, "b", null, null, 64, 3, 400);
        var ranges = await planner.PlanAsync(CancellationToken.None);
                Assert.IsTrue(a.MaxInFlight >= 4 || b.MaxInFlight >= 4, $"probes should overlap, peak per bucket {a.MaxInFlight}/{b.MaxInFlight}");
        Assert.IsTrue(a.MaxInFlight <= PartitionPlanner.MaxConcurrentPlannerRequests && b.MaxInFlight <= PartitionPlanner.MaxConcurrentPlannerRequests, "more requests in flight than the planner cap");
        Assert.IsTrue(planner.RequestsUsed <= 400);
        Assert.IsNull(PartitionPlanner.ValidateTiling(ranges));
    }

    [TestMethod]
    public void PlanningSetting_IsValidated_Overridden_AndRead()
    {
        Assert.AreEqual(60, new ScanOptions().MaxPlanningSeconds);
        var job = new ComparisonJob { Performance = { MaxPlanningSeconds = 5 } };
        Assert.AreEqual(5, new ScanOptions().ForJob(job).MaxPlanningSeconds);
        Assert.AreEqual(0, new ScanOptions().ForJob(new ComparisonJob { Performance = { MaxPlanningSeconds = 0 } }).MaxPlanningSeconds);
        Assert.IsNull(ScanOptions.Validate(new JobPerformanceSettings { MaxPlanningSeconds = 0 }));
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { MaxPlanningSeconds = -1 }));
        Assert.IsNotNull(ScanOptions.Validate(new JobPerformanceSettings { MaxPlanningSeconds = ScanOptions.MaxPlanningSecondsLimit + 1 }));
        Assert.IsFalse(new JobPerformanceSettings { MaxPlanningSeconds = 3 }.IsEmpty);

        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["S3Compare:MaxPlanningSeconds"] = "15" }).Build();
        Assert.AreEqual(15, HardeningSettings.Scan(config).MaxPlanningSeconds);
        var bad = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["S3Compare:MaxPlanningSeconds"] = "-4" }).Build();
        Assert.AreEqual(60, HardeningSettings.Scan(bad).MaxPlanningSeconds);
    }
}

[TestClass]
public class ScanVisibilityTests : TestDbBase
{
    private static S3ObjectSummary Obj(string key) => new() { Key = key, Size = 1, ETag = "e", LastModified = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), StorageClass = "STANDARD" };

    private static ComparisonJob NewJob() => new()
    {
        Name = "visibility",
        ReplicationGracePeriodMinutes = 0,
        DetectSequenceGaps = false,
        BucketA = { BucketName = "bucket-a" },
        BucketB = { BucketName = "bucket-b" }
    };

    private static ScanOptions Options(int partitions, int pageSize = 200) => new()
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

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 15000)
    {
        var sw = Stopwatch.StartNew();
        while (!condition())
        {
            if (sw.ElapsedMilliseconds > timeoutMs) Assert.Fail("condition not reached in time");
            await Task.Delay(20);
        }
    }

    [TestMethod]
    public async Task PhaseAndPlannerProgress_AreReported_WhilePlanning_AndLogged()
    {
        var keys = Enumerable.Range(0, 1500).Select(i => $"file_{i:00000}.dat").ToList();
        var job = NewJob();
        var run = await NewRunAsync(job);
        var options = Options(4);
        options.MaxPlanningSeconds = 0;
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: options,
            listerA: new FakeS3Lister(keys.Select(Obj), TimeSpan.FromMilliseconds(20)), listerB: new FakeS3Lister(keys.Select(Obj), TimeSpan.FromMilliseconds(20)));

        var task = Task.Run(() => engine.RunAsync());
        ScanDiagnosticsSnapshot? seen = null;
        await WaitUntilAsync(() =>
        {
            var d = engine.GetDiagnostics(false);
            if (d.UnderlyingPhase == "Planning" && d.Planner.RequestsUsed > 3) { seen = d; return true; }
            return false;
        });
        await task;

        Assert.IsNotNull(seen);
        Assert.IsTrue(seen!.PhaseText.StartsWith("PLANNING: ", StringComparison.Ordinal), seen.PhaseText);
        StringAssert.Contains(seen.PhaseText, "requests");
        StringAssert.Contains(seen.PhaseText, "candidates");
        Assert.AreEqual(options.MaxDiscoveryRequests, seen.Planner.RequestBudget);
        Assert.IsTrue(seen.BucketA.Requests > 0 && seen.BucketA.LastLatencyMs >= 15, $"latency {seen.BucketA.LastLatencyMs}");

        var final = engine.GetDiagnostics();
        Assert.AreEqual("Finalizing", final.UnderlyingPhase);
        Assert.IsTrue(final.Planner.Finished);
        Assert.IsTrue(final.Partitions.Count > 1);
        Assert.IsTrue(final.Partitions.All(p => p.State == "done"));
        Assert.AreEqual(keys.Count, final.Partitions.Sum(p => p.ObjectsCompared));

        var logs = (await AuditRepo.GetRecentLogsAsync(500)).Select(l => l.Message).ToList();
        Assert.IsTrue(logs.Any(m => m.StartsWith("Planning started", StringComparison.Ordinal) && m.Contains("request budget") && m.Contains("rate limit") && m.Contains("prefix A")), "planning start is logged");
        Assert.IsTrue(logs.Any(m => m.StartsWith("Planner step:", StringComparison.Ordinal)), "planner steps are logged");
        Assert.IsTrue(logs.Any(m => m.StartsWith("Scan phase: Scanning", StringComparison.Ordinal)));
        Assert.IsTrue(logs.Any(m => m.StartsWith("Scan step: Workers started", StringComparison.Ordinal)));
        Assert.IsTrue(logs.Any(m => m.StartsWith("Scan step: Creating S3 clients", StringComparison.Ordinal)));
        Assert.IsTrue(logs.Any(m => m.StartsWith("Partition plan for run", StringComparison.Ordinal)));
    }

    [TestMethod]
    public async Task OnePartition_SkipsPlanningEntirely()
    {
        var keys = Enumerable.Range(0, 300).Select(i => $"d{i / 30:00}/f{i:000}").ToList();
        var a = new FakeS3Lister(keys.Select(Obj));
        var job = NewJob();
        var run = await NewRunAsync(job);
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(1), listerA: a, listerB: new FakeS3Lister(keys.Select(Obj)));
        await engine.RunAsync();

        Assert.IsFalse(a.Requests.Any(r => r.Delimiter == "/" || r.MaxKeys == 1), "no planner request may be sent");
        var d = engine.GetDiagnostics();
        Assert.IsFalse(d.Planner.Started);
        Assert.AreEqual(1, d.PartitionsTotal);
        var logs = (await AuditRepo.GetRecentLogsAsync(200)).Select(l => l.Message).ToList();
        Assert.IsTrue(logs.Any(m => m.StartsWith("Planning skipped", StringComparison.Ordinal)));
        Assert.IsFalse(logs.Any(m => m.StartsWith("Planning started", StringComparison.Ordinal)));
    }

    [TestMethod]
    public async Task DiagnosticsPartitionRows_AreCapped()
    {
        var keys = Enumerable.Range(0, 6000).Select(i => $"k{i:00000}").ToList();
        var job = NewJob();
        var run = await NewRunAsync(job);
        var options = Options(64, pageSize: 50);
        options.PartitionOversubscription = 64; // target 4096 ranges
        options.MaxDiscoveryRequests = 2000;
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: options, listerA: new FakeS3Lister(keys.Select(Obj)), listerB: new FakeS3Lister(keys.Select(Obj)));
        await engine.RunAsync();

        var d = engine.GetDiagnostics();
        Assert.IsTrue(d.PartitionsTotal > ScanDiagnosticsFormatter.MaxPartitionRows, $"ranges: {d.PartitionsTotal}");
        Assert.AreEqual(ScanDiagnosticsFormatter.MaxPartitionRows, d.Partitions.Count);
        Assert.AreEqual(d.PartitionsTotal - d.Partitions.Count, d.PartitionsOmitted);
        StringAssert.Contains(ScanDiagnosticsFormatter.Summary(d), "PARTITIONS");
    }

    private ScanExecutionCoordinator NewCoordinator(Func<ComparisonJob, ScanRun, ScanCheckpoint?, ServiceComparisonEngine> factory, TimeSpan watchdog)
    {
        var alerts = new S3CompareService.Core.Notifications.AlertNotificationService(AuditRepo);
        return new ScanExecutionCoordinator(JobRepo, RunRepo, DiffRepo, AuditRepo, alerts, NullLogger<ScanExecutionCoordinator>.Instance)
        {
            CancelWatchdogTimeout = watchdog,
            EngineFactory = factory
        };
    }

    [TestMethod]
    public async Task Cancel_WithAListerThatIgnoresCancellation_EndsCancelled_AndShowsTheInFlightRequest()
    {
        var job = NewJob();
        await JobRepo.SaveAsync(job);
        var hung = new MisbehavingLister(new FakeS3Lister(new[] { Obj("a") }))
        {
            Behaviour = (_, _, _, _, _) => new TaskCompletionSource().Task // ignores the token, never returns
        };
        var coordinator = NewCoordinator((j, r, cp) => new ServiceComparisonEngine(j, r, RunRepo, DiffRepo, AuditRepo, cp, options: Options(4), listerA: hung, listerB: new FakeS3Lister(Array.Empty<S3ObjectSummary>())), TimeSpan.FromSeconds(30));

        var started = await coordinator.TriggerScanAsync(job.Id);
        Assert.IsTrue(started.Success, started.Message);
        var ctx = coordinator.GetActiveScan(started.RunId)!;

        await WaitUntilAsync(() => ctx.Engine.GetDiagnostics(false).OldestS3 != null);
        var diag = ctx.Engine.GetDiagnostics(false);
        StringAssert.Contains(diag.OldestS3!.Description, "ListObjectsV2");
        Assert.AreEqual("Planning", diag.UnderlyingPhase);

        Assert.IsTrue(coordinator.CancelScan(started.RunId));
        await ctx.ExecutionTask.WaitAsync(TimeSpan.FromSeconds(10)); // ends through the engine itself, long before the watchdog
        await WaitUntilAsync(() => !coordinator.IsRunActive(started.RunId));
        Assert.AreEqual(RunStatus.Cancelled, (await RunRepo.GetByIdAsync(started.RunId))!.Status);
        Assert.IsTrue((await AuditRepo.GetRecentLogsAsync(200)).Any(l => l.Level == "INFO" && l.Message.StartsWith("Cancel requested", StringComparison.Ordinal)));
        Assert.IsFalse(coordinator.IsJobRunning(job.Id));
    }

    [TestMethod]
    public async Task Cancel_OfAnEngineBlockedOnAThread_IsReportedByTheWatchdog_AndTheEngineCannotOverwriteIt()
    {
        var job = NewJob();
        await JobRepo.SaveAsync(job);
        using var release = new ManualResetEventSlim(false);
        var blocked = new MisbehavingLister(new FakeS3Lister(new[] { Obj("a") }))
        {
            Behaviour = (_, _, _, _, _) => { release.Wait(); return Task.CompletedTask; } // blocks the calling thread: nothing can cancel it
        };
        var coordinator = NewCoordinator((j, r, cp) => new ServiceComparisonEngine(j, r, RunRepo, DiffRepo, AuditRepo, cp, options: Options(4), listerA: blocked, listerB: new FakeS3Lister(Array.Empty<S3ObjectSummary>())), TimeSpan.FromMilliseconds(200));

        var started = await coordinator.TriggerScanAsync(job.Id);
        var ctx = coordinator.GetActiveScan(started.RunId)!;
        await WaitUntilAsync(() => blocked.Calls > 0);
        await WaitUntilAsync(() => ctx.Engine.GetDiagnostics(false).OldestS3 != null);

        coordinator.CancelScan(started.RunId);
        Assert.IsNotNull(ctx.CancelRequestedUtc);
        await WaitUntilAsync(() => !coordinator.IsRunActive(started.RunId), 15000);

        var stored = (await RunRepo.GetByIdAsync(started.RunId))!;
        Assert.AreEqual(RunStatus.Cancelled, stored.Status);
        Assert.IsFalse(coordinator.IsJobRunning(job.Id), "the job can run again");
        var logs = await AuditRepo.GetRecentLogsAsync(200);
        Assert.IsTrue(logs.Any(l => l.Level == "INFO" && l.Message.StartsWith("Cancel requested", StringComparison.Ordinal)));
        var error = logs.FirstOrDefault(l => l.Level == "ERROR" && l.Message.Contains("did not stop"));
        Assert.IsNotNull(error, "the watchdog logs an ERROR");
        StringAssert.Contains(error!.Message, "ListObjectsV2");
        Assert.IsTrue(ctx.Engine.IsDetached);

        // Let the stuck call return: the detached engine must not change the run or add rows.
        release.Set();
        await ctx.ExecutionTask.WaitAsync(TimeSpan.FromSeconds(20));
        var after = (await RunRepo.GetByIdAsync(started.RunId))!;
        Assert.AreEqual(RunStatus.Cancelled, after.Status);
        Assert.AreEqual(0, await DiffRepo.GetDifferenceCountAsync(started.RunId));
    }
}
