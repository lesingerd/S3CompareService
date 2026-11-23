using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http;
using System.Reflection;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;

namespace S3CompareService.Tests;

/// <summary>Errors are classified by what they meant (recovered, skipped, degraded, recorded, failed); only a failure is an error.</summary>
[TestClass]
public class ErrorOutcomeTests : TestDbBase
{
    private static S3ObjectSummary Obj(string key) => new() { Key = key, Size = 1, ETag = "e", LastModified = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), StorageClass = "STANDARD" };
    private static List<string> FlatKeys(int n) => Enumerable.Range(0, n).Select(i => $"file_{i:00000}.dat").ToList();

    private static ComparisonJob NewJob() => new()
    {
        Name = "outcomes",
        ReplicationGracePeriodMinutes = 0,
        DetectSequenceGaps = false,
        BucketA = { BucketName = "bucket-a" },
        BucketB = { BucketName = "bucket-b" }
    };

    private static ScanOptions Options(int partitions) => new()
    {
        MaxParallelPartitions = partitions,
        ListPageSize = 200,
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

    // ---- the collector ----

    [TestMethod]
    public void Issues_SummaryAndTopIssue_PreferTheMostSevere_ThenTheMostRecent()
    {
        var issues = new ScanIssues();
        Assert.IsNull(issues.Top());
        Assert.AreEqual("Issues: 0 failed · 0 recovered · 0 skipped · 0 degraded", issues.Summary().Text);

        issues.Record(IssueOutcome.Recovered, "r1", 2);
        issues.Record(IssueOutcome.Skipped, "s1");
        issues.Record(IssueOutcome.Skipped, "s2");
        Assert.AreEqual("s2", issues.Top()!.Text, "skipped beats recovered; the most recent of equals wins");
        issues.Record(IssueOutcome.Degraded, "d1");
        issues.Record(IssueOutcome.Recorded, "x1");
        Assert.AreEqual("d1", issues.Top()!.Text, "degraded beats recorded");
        issues.Record(IssueOutcome.Failed, "f1");
        issues.Record(IssueOutcome.Recovered, "r2", 3);
        var top = issues.Top()!;
        Assert.AreEqual("Failed", top.Outcome);
        Assert.AreEqual("FAILED: f1", top.Display);

        var s = issues.Summary();
        Assert.AreEqual(1, s.Failed);
        Assert.AreEqual(2, s.Recovered);
        Assert.AreEqual(2, s.Skipped);
        Assert.AreEqual(1, s.Degraded);
        Assert.AreEqual("Issues: 1 failed · 2 recovered · 2 skipped · 1 degraded · 1 recorded", s.Text);
        Assert.AreEqual("RECOVERED (attempt 3): r2", issues.Recent()[0].Display);
    }

    [TestMethod]
    public void Issues_RecentList_And_Text_AreBounded_AndLoggingIsCapped()
    {
        var logs = new ConcurrentQueue<(string Level, string Message)>();
        var issues = new ScanIssues { Log = (l, m) => logs.Enqueue((l, m)) };
        for (int i = 0; i < 500; i++) issues.Record(IssueOutcome.Skipped, new string('x', 5000));
        issues.Record(IssueOutcome.Failed, "boom");
        issues.Record(IssueOutcome.Degraded, "slow");

        Assert.AreEqual(500, issues.Summary().Skipped, "counting is not capped");
        Assert.AreEqual(ScanIssues.MaxRecent, issues.Recent().Count);
        Assert.IsTrue(issues.Recent().All(i => i.Text.Length <= ScanIssues.MaxTextLength));
        Assert.AreEqual(1, logs.Count(l => l.Message.Contains("further skipped issues are counted but not logged individually")));
        Assert.AreEqual(ScanIssues.MaxLoggedPerOutcome + 1, logs.Count(l => l.Message.StartsWith("SKIPPED", StringComparison.Ordinal)));
        Assert.IsTrue(logs.Any(l => l.Level == "ERROR" && l.Message == "FAILED: boom"), "failed is logged as ERROR");
        Assert.IsTrue(logs.Any(l => l.Level == "WARN" && l.Message == "DEGRADED: slow"), "everything else is a WARN");
        Assert.IsFalse(logs.Any(l => l.Level == "ERROR" && !l.Message.StartsWith("FAILED", StringComparison.Ordinal)));
    }

    [TestMethod]
    public void Snapshot_WithIssues_RoundTripsThroughJson_ForStatusAndTheUi()
    {
        var issues = new ScanIssues();
        issues.Record(IssueOutcome.Recovered, "ListObjectsV2 bucket B: timed out after 30 s — no impact", 2);
        issues.Record(IssueOutcome.Skipped, "planner request");
        var snap = new ScanDiagnosticsSnapshot { Issues = issues.Summary(), TopIssue = issues.Top(), RecentIssues = issues.Recent() };

        var web = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var back = JsonSerializer.Deserialize<ScanDiagnosticsSnapshot>(JsonSerializer.Serialize(snap, web), web)!;

        Assert.AreEqual("Issues: 0 failed · 1 recovered · 1 skipped · 0 degraded", back.Issues.Text);
        Assert.AreEqual("SKIPPED: planner request", back.TopIssue!.Display);
        Assert.AreEqual(2, back.RecentIssues.Count);
        Assert.AreEqual("RECOVERED (attempt 2): ListObjectsV2 bucket B: timed out after 30 s — no impact", back.RecentIssues[1].Display);
        StringAssert.Contains(ScanDiagnosticsFormatter.Summary(back), "RECENT ISSUES");
    }

    // ---- S3 retry that succeeds ----

    public class FlakyProxy : DispatchProxy
    {
        public int Requests;
        public int FailFirst = 1;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name != nameof(IAmazonS3.ListObjectsV2Async)) throw new NotSupportedException(targetMethod?.Name);
            int n = Interlocked.Increment(ref Requests);
            if (n <= FailFirst) return Task.FromException<ListObjectsV2Response>(new HttpRequestException("connection reset (simulated)"));
            return Task.FromResult(new ListObjectsV2Response { IsTruncated = false, S3Objects = [new S3Object { Key = "k", Size = 1, ETag = "\"e\"", LastModified = DateTime.UtcNow }] });
        }
    }

    [TestMethod]
    public async Task RetriedRequest_ThatSucceeds_IsRecovered_NotAnError()
    {
        var s3 = DispatchProxy.Create<IAmazonS3, FlakyProxy>();
        var issues = new ScanIssues();
        var diag = new ScanDiagnostics();
        diag.ConfigureBuckets("a", "b", 0);
        var lister = new InstrumentedLister(new S3Lister(s3, maxAttempts: 3), diag.BucketB, diag.InFlight, issues);

        var page = await lister.ListPageAsync("b", "pre/", null, null, "/", 1000, CancellationToken.None);

        Assert.AreEqual(1, page.Objects.Count);
        var summary = issues.Summary();
        Assert.AreEqual(1, summary.Recovered);
        Assert.AreEqual(0, summary.Failed);
        var top = issues.Top()!;
        Assert.AreEqual("Recovered", top.Outcome);
        Assert.AreEqual(2, top.Attempt);
        StringAssert.StartsWith(top.Display, "RECOVERED (attempt 2): ListObjectsV2 bucket B");
        StringAssert.Contains(top.Display, "connection reset");
        StringAssert.EndsWith(top.Display, "no impact");
        var stats = diag.BucketB.Snapshot();
        Assert.AreEqual(1, stats.Retried);
        Assert.AreEqual(1, stats.Recovered);
        Assert.AreEqual(0, stats.Errors, "the request succeeded, so no failed request is counted");
        StringAssert.Contains(ScanDiagnosticsFormatter.Summary(new ScanDiagnosticsSnapshot { BucketB = stats }), "retried 1 (recovered 1) · failed 0");
    }

    [TestMethod]
    public async Task RetriedRequest_ThatKeepsFailing_IsAFailedRequest_AndLeavesItsClassificationToTheCaller()
    {
        var s3 = DispatchProxy.Create<IAmazonS3, FlakyProxy>();
        ((FlakyProxy)(object)s3).FailFirst = 99;
        var issues = new ScanIssues();
        var diag = new ScanDiagnostics();
        diag.ConfigureBuckets("a", "b", 0);
        var lister = new InstrumentedLister(new S3Lister(s3, maxAttempts: 2), diag.BucketA, diag.InFlight, issues);

        await Assert.ThrowsAsync<HttpRequestException>(() => lister.ListPageAsync("a", null, null, null, null, 10, CancellationToken.None));

        Assert.AreEqual(0, issues.Summary().Total, "nothing is claimed recovered or fatal at this level");
        var stats = diag.BucketA.Snapshot();
        Assert.AreEqual(1, stats.Retried);
        Assert.AreEqual(0, stats.Recovered);
        Assert.AreEqual(1, stats.Errors);
    }

    // ---- planner ----

    [TestMethod]
    public async Task PlannerRequestSkipped_IsSkipped_NotFailed_AndPlanningStillBuildsAFullPlan()
    {
        var keys = FlatKeys(3000);
        var bad = new FakeS3Lister(keys.Select(Obj)) { Fault = call => call == 2 ? new InvalidOperationException("boom (simulated)") : null };
        var issues = new ScanIssues();
        var planner = new PartitionPlanner(bad, "a", null, null, new FakeS3Lister(keys.Select(Obj)), "b", null, null, 32, 3, 500, null, issues: issues);

        var ranges = await planner.PlanAsync(CancellationToken.None);

        Assert.IsNull(PartitionPlanner.ValidateTiling(ranges));
        var s = issues.Summary();
        Assert.IsTrue(s.Skipped >= 1);
        Assert.AreEqual(0, s.Failed);
        var top = issues.Top()!;
        Assert.AreEqual("Skipped", top.Outcome);
        StringAssert.StartsWith(top.Display, "SKIPPED: planner request bucket A");
        StringAssert.Contains(top.Display, "boom (simulated)");
        StringAssert.Contains(top.Display, "planning continued; the plan is still complete and results are unaffected");
        StringAssert.StartsWith(planner.LastError, "SKIPPED: planner request bucket A");
    }

    [TestMethod]
    public async Task PlannerTimeLimit_IsDegraded_NotFailed()
    {
        var keys = FlatKeys(5000);
        var issues = new ScanIssues();
        var planner = new PartitionPlanner(new FakeS3Lister(keys.Select(Obj), TimeSpan.FromMilliseconds(60)), "a", null, null,
            new FakeS3Lister(keys.Select(Obj), TimeSpan.FromMilliseconds(60)), "b", null, null, 64, 3, 100_000, null,
            maxPlanningTime: TimeSpan.FromMilliseconds(500), issues: issues);

        await planner.PlanAsync(CancellationToken.None);

        Assert.IsTrue(planner.TimedOut);
        var s = issues.Summary();
        Assert.AreEqual(1, s.Degraded);
        Assert.AreEqual(0, s.Failed);
        Assert.AreEqual(0, s.Skipped, "a request cut short by the time limit is not a skipped request");
        StringAssert.StartsWith(issues.Top()!.Display, "DEGRADED: planner time limit of ");
    }

    // ---- whole scans ----

    [TestMethod]
    public async Task Scan_WithASkippedPlannerRequest_ShowsNoFailure_AndLogsAWarn()
    {
        var keys = FlatKeys(3000);
        var job = NewJob();
        var run = await NewRunAsync(job);
        var a = new FakeS3Lister(keys.Select(Obj)) { Fault = call => call == 2 ? new InvalidOperationException("planner boom (simulated)") : null };
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(4), listerA: a, listerB: new FakeS3Lister(keys.Select(Obj)));

        await engine.RunAsync();

        var d = engine.GetDiagnostics();
        Assert.IsTrue(d.Issues.Skipped >= 1, d.Issues.Text);
        Assert.AreEqual(0, d.Issues.Failed);
        Assert.IsNull(d.LastError, "a skipped request must not appear as the last error");
        Assert.AreEqual("Skipped", d.TopIssue!.Outcome);
        StringAssert.StartsWith(d.Planner.LastError, "SKIPPED:");
        StringAssert.Contains(ScanDiagnosticsFormatter.Summary(d), "RECENT ISSUES");
        Assert.AreEqual(RunStatus.Completed, (await RunRepo.GetByIdAsync(run.Id))!.Status);

        await WaitUntilAsync(() => AuditRepo.GetRecentLogsAsync(500).GetAwaiter().GetResult().Any(l => l.Message.StartsWith("SKIPPED:", StringComparison.Ordinal)));
        var logs = await AuditRepo.GetRecentLogsAsync(500);
        Assert.IsTrue(logs.Where(l => l.Message.StartsWith("SKIPPED:", StringComparison.Ordinal)).All(l => l.Level == "WARN"));
        Assert.IsFalse(logs.Any(l => l.Level == "ERROR"), "no ERROR line for a harmless issue");
    }

    [TestMethod]
    public async Task Scan_ThatFails_RecordsAFailedIssue_AndOnlyThatIsTheLastError()
    {
        var keys = FlatKeys(100);
        var job = NewJob();
        var run = await NewRunAsync(job);
        var a = new FakeS3Lister(keys.Select(Obj)) { Fault = _ => new InvalidOperationException("listing exploded (simulated)") };
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo, options: Options(1), listerA: a, listerB: new FakeS3Lister(keys.Select(Obj)));

        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.RunAsync());

        var d = engine.GetDiagnostics();
        Assert.AreEqual(1, d.Issues.Failed);
        Assert.AreEqual("Failed", d.TopIssue!.Outcome);
        StringAssert.StartsWith(d.LastError, "FAILED: scan run failed and stopped");
        StringAssert.Contains(d.LastError, "listing exploded");
        Assert.AreEqual(RunStatus.Failed, (await RunRepo.GetByIdAsync(run.Id))!.Status);

        await WaitUntilAsync(() => AuditRepo.GetRecentLogsAsync(500).GetAwaiter().GetResult().Any(l => l.Level == "ERROR" && l.Message.StartsWith("FAILED:", StringComparison.Ordinal)));
    }
}
