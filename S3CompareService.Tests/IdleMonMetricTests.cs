using S3CompareService.Core.Models;
using S3CompareService.Worker.IdleMon;
using static S3CompareService.Tests.IdleMonTestData;

namespace S3CompareService.Tests;

[TestClass]
public class IdleMonMetricTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);

    private static List<IdleMonMessage> Build(IReadOnlyList<JobState> states, ResultSendTracker? tracker = null, IdleMonOptions? options = null, int scans = 0) =>
        MetricBuilder.Build(states, scans, 0, Now, options ?? Options(), tracker ?? new ResultSendTracker());

    private static bool HasResultMetrics(IEnumerable<IdleMonMessage> metrics) => metrics.Any(m => m.Name == "s3c.run.objects_compared");

    private static JobState EndedAgo(ComparisonJob job, TimeSpan ago) =>
        State(job, null, Run(job.Id, RunStatus.Completed, Now.UtcDateTime - ago - TimeSpan.FromMinutes(10)));

    [TestMethod]
    public void PerJobMetrics_HaveCatalogNamesUnitsAndTags()
    {
        var job = Job();
        var run = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-40), TimeSpan.FromMinutes(10), r =>
        {
            r.FilteredDifferencesCount = 7; r.InFlightDifferencesCount = 3; r.MissingInBucketBCount = 4; r.MissingInBucketACount = 1;
            r.SizeMismatchCount = 2; r.ETagMismatchCount = 0; r.ContentMismatchCount = 0; r.SequenceGapCount = 0;
            r.ObjectsCompared = 2000; r.MatchedCount = 1990; r.DurationSeconds = 600; r.BucketABytesRead = 1000; r.BucketBBytesRead = 500;
        });
        var metrics = Build([State(job, null, run)]);

        double Value(string name) => metrics.Single(m => m.Name == name).Value!.Value;
        Assert.AreEqual(30 * 60, Value("s3c.job.last_success.age"), 0.5);
        Assert.AreEqual(0, Value("s3c.job.consecutive_failures"));
        Assert.AreEqual(7, Value("s3c.run.disparities.confirmed"));
        Assert.AreEqual(3, Value("s3c.run.disparities.inflight"));
        Assert.AreEqual(4, Value("s3c.run.missing_in_b"));
        Assert.AreEqual(1, Value("s3c.run.missing_in_a"));
        Assert.AreEqual(2, Value("s3c.run.size_mismatch"));
        Assert.AreEqual(2000, Value("s3c.run.objects_compared"));
        Assert.AreEqual(99.5, Value("s3c.run.match_rate"), 0.0001);
        Assert.AreEqual(600, Value("s3c.run.duration"));
        Assert.AreEqual(1500, Value("s3c.run.bytes_read"));
        Assert.AreEqual("percent", metrics.Single(m => m.Name == "s3c.run.match_rate").Unit);
        Assert.AreEqual("bytes", metrics.Single(m => m.Name == "s3c.run.bytes_read").Unit);
        Assert.AreEqual("seconds", metrics.Single(m => m.Name == "s3c.run.duration").Unit);
        Assert.AreEqual("seconds", metrics.Single(m => m.Name == "s3c.job.last_success.age").Unit);

        var tags = metrics.First(m => m.Name == "s3c.run.duration").Tags!;
        Assert.AreEqual("job1", tags["jobId"]);
        Assert.AreEqual("Archive vs Wasabi", tags["job"]);
        Assert.IsTrue(metrics.Where(m => m.Name!.StartsWith("s3c.run.") || m.Name.StartsWith("s3c.job.")).All(m => m.Tags is not null));
        Assert.IsTrue(metrics.Where(m => m.Name!.StartsWith("s3c.jobs.") || m.Name == "s3c.scans.active").All(m => m.Tags is null), "service level metrics have no job tag");
    }

    [TestMethod]
    public void ServiceLevelMetrics_CountEnabledFailingOverdueAndDisparities()
    {
        var okJob = Job("a", "A");
        var failing = Job("b", "B");
        var disabled = Job("c", "C", enabled: false);
        var withDisparities = Job("d", "D");
        var states = new List<JobState>
        {
            State(okJob, null, Run("a", RunStatus.Completed, Now.UtcDateTime.AddMinutes(-30))),
            State(failing, null, Run("b", RunStatus.Failed, Now.UtcDateTime.AddMinutes(-30))),
            State(disabled, null, Run("c", RunStatus.Failed, Now.UtcDateTime.AddMinutes(-30))),
            State(withDisparities, null, Run("d", RunStatus.Completed, Now.UtcDateTime.AddMinutes(-30), null, r => r.FilteredDifferencesCount = 2)),
        };
        var metrics = MetricBuilder.Build(states, 2, 1, Now, Options(), new ResultSendTracker());
        double Value(string name) => metrics.Single(m => m.Name == name).Value!.Value;
        Assert.AreEqual(3, Value("s3c.jobs.enabled"));
        Assert.AreEqual(1, Value("s3c.jobs.failing"));
        Assert.AreEqual(1, Value("s3c.jobs.overdue"));
        Assert.AreEqual(1, Value("s3c.jobs.with_disparities"));
        Assert.AreEqual(2, Value("s3c.scans.active"));
        Assert.IsFalse(metrics.Any(m => m.Tags is not null && m.Tags["jobId"] == "c"), "a disabled job has no per-job metrics");
    }

    [TestMethod]
    public void MatchRate_IsOmitted_WhenNoObjectsWereCompared()
    {
        var job = Job();
        var run = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-20), null, r => { r.ObjectsCompared = 0; r.MatchedCount = 0; });
        var metrics = Build([State(job, null, run)]);
        Assert.IsFalse(metrics.Any(m => m.Name == "s3c.run.match_rate"));
        Assert.AreEqual(0, metrics.Single(m => m.Name == "s3c.run.objects_compared").Value);
    }

    [TestMethod]
    public void ResultMetrics_FirstSendUsesRunEndTime_LaterSendsUseSendTime()
    {
        var job = Job();
        var run = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-30));
        var state = State(job, null, run);
        var tracker = new ResultSendTracker();

        var first = Build([state], tracker).First(m => m.Name == "s3c.run.objects_compared");
        Assert.AreEqual(IdleMonWire.Timestamp(new DateTimeOffset(run.EndTimeUtc!.Value, TimeSpan.Zero)), first.Timestamp);

        var second = Build([state], tracker).First(m => m.Name == "s3c.run.objects_compared");
        Assert.AreEqual(IdleMonWire.Timestamp(Now), second.Timestamp);

        var newer = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-5), TimeSpan.FromMinutes(1));
        var third = Build([State(job, null, run, newer)], tracker).First(m => m.Name == "s3c.run.objects_compared");
        Assert.AreEqual(IdleMonWire.Timestamp(new DateTimeOffset(newer.EndTimeUtc!.Value, TimeSpan.Zero)), third.Timestamp, "a new run is a first send again");
    }

    [TestMethod]
    [DataRow(60, 179, true)]    // interval 60 min: window 3 x 60 = 180 min
    [DataRow(60, 181, false)]
    [DataRow(1, 9, true)]       // 3 x 1 min is below the 10 minute minimum
    [DataRow(1, 11, false)]
    [DataRow(1440, 3 * 1440 - 5, true)]
    [DataRow(1440, 3 * 1440 + 5, false)]
    public void ResultMetrics_ResendWindow_FollowsIntervalCadence(int intervalMinutes, int endedMinutesAgo, bool expected)
    {
        var job = Job(intervalMinutes: intervalMinutes);
        var metrics = Build([EndedAgo(job, TimeSpan.FromMinutes(endedMinutesAgo))]);
        Assert.AreEqual(expected, HasResultMetrics(metrics));
        Assert.IsTrue(metrics.Any(m => m.Name == "s3c.job.last_success.age"), "job metrics keep going after the window");
        Assert.IsTrue(metrics.Any(m => m.Name == "s3c.job.consecutive_failures"));
    }

    [TestMethod]
    [DataRow(71 * 60, true)]    // daily cron: cadence 24 h, window 72 h
    [DataRow(73 * 60, false)]
    public void ResultMetrics_ResendWindow_FollowsCronCadence(int endedMinutesAgo, bool expected)
    {
        var job = Job();
        job.ScheduleType = JobScheduleType.CronExpression;
        job.CronExpression = "0 2 * * *";
        Assert.AreEqual(TimeSpan.FromHours(24), JobCadence.Of(job, Now.UtcDateTime));
        Assert.AreEqual(expected, HasResultMetrics(Build([EndedAgo(job, TimeSpan.FromMinutes(endedMinutesAgo))])));
    }

    [TestMethod]
    [DataRow(71 * 60, true)]    // manual: cadence 24 h
    [DataRow(73 * 60, false)]
    public void ResultMetrics_ResendWindow_ForManualJobsIs24HourCadence(int endedMinutesAgo, bool expected)
    {
        var job = Job();
        job.ScheduleType = JobScheduleType.Manual;
        Assert.AreEqual(expected, HasResultMetrics(Build([EndedAgo(job, TimeSpan.FromMinutes(endedMinutesAgo))])));
    }

    [TestMethod]
    public void StaleResultMultiplier_IsConfigurable()
    {
        var job = Job(intervalMinutes: 60);
        var state = EndedAgo(job, TimeSpan.FromMinutes(100));
        Assert.IsTrue(HasResultMetrics(Build([state])));
        Assert.IsFalse(HasResultMetrics(Build([state], options: Options(o => o.StaleResultMultiplier = 1))));
    }

    [TestMethod]
    public void FailedRun_PartialCountsAreNeverSent()
    {
        var job = Job();
        var failedOnly = State(job, null, Run(job.Id, RunStatus.Failed, Now.UtcDateTime.AddMinutes(-20), null, r => { r.ObjectsCompared = 777; r.FilteredDifferencesCount = 99; }));
        var metrics = Build([failedOnly]);
        Assert.IsFalse(metrics.Any(m => m.Name!.StartsWith("s3c.run.")), "no s3c.run.* from a failed run");
        Assert.IsFalse(metrics.Any(m => m.Name == "s3c.job.last_success.age"), "no completed run, no age");
        Assert.AreEqual(1, metrics.Single(m => m.Name == "s3c.job.consecutive_failures").Value);

        // An older completed run still describes the last good result; the failed run's counts do not replace it.
        var good = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-60), null, r => { r.ObjectsCompared = 1234; r.FilteredDifferencesCount = 5; });
        var failed = Run(job.Id, RunStatus.Failed, Now.UtcDateTime.AddMinutes(-20), null, r => { r.ObjectsCompared = 777; r.FilteredDifferencesCount = 99; });
        var both = Build([State(job, null, good, failed)]);
        Assert.AreEqual(1234, both.Single(m => m.Name == "s3c.run.objects_compared").Value);
        Assert.AreEqual(5, both.Single(m => m.Name == "s3c.run.disparities.confirmed").Value);
    }

    [TestMethod]
    public void ConsecutiveFailures_CountFailedRunsSinceLastCompleted()
    {
        var job = Job();
        var t = Now.UtcDateTime;
        var state = State(job, null,
            Run(job.Id, RunStatus.Failed, t.AddMinutes(-10)),
            Run(job.Id, RunStatus.Cancelled, t.AddMinutes(-70)),
            Run(job.Id, RunStatus.Failed, t.AddMinutes(-130)),
            Run(job.Id, RunStatus.Completed, t.AddMinutes(-190)),
            Run(job.Id, RunStatus.Failed, t.AddMinutes(-250)));
        Assert.AreEqual(2, state.ConsecutiveFailures);
        Assert.AreEqual(0, State(job, null, Run(job.Id, RunStatus.Completed, t.AddMinutes(-10)), Run(job.Id, RunStatus.Failed, t.AddMinutes(-70))).ConsecutiveFailures);
    }

    [TestMethod]
    public void ScanMetrics_AreSentWhileScanning_AndPerSecondIsOmittedWhenPaused()
    {
        var job = Job();
        var running = State(job, new ActiveScanInfo(job.Id, "r1", 5000, 250.5, false));
        var metrics = Build([running], scans: 1);
        Assert.AreEqual(5000, metrics.Single(m => m.Name == "s3c.scan.objects_compared").Value);
        Assert.AreEqual(250.5, metrics.Single(m => m.Name == "s3c.scan.objects_per_second").Value);
        Assert.AreEqual("per_second", metrics.Single(m => m.Name == "s3c.scan.objects_per_second").Unit);

        var paused = Build([State(job, new ActiveScanInfo(job.Id, "r1", 5000, 250.5, true))], scans: 1);
        Assert.IsTrue(paused.Any(m => m.Name == "s3c.scan.objects_compared"));
        Assert.IsFalse(paused.Any(m => m.Name == "s3c.scan.objects_per_second"));

        Assert.IsFalse(Build([State(job)]).Any(m => m.Name!.StartsWith("s3c.scan.")), "no scan, no scan metrics");
    }

    [TestMethod]
    public void AllMetrics_PassContractValidation()
    {
        var job = Job();
        var run = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-20));
        var reporter = new CapturingReporter(new FakeClock(Now));
        foreach (var m in Build([State(job, new ActiveScanInfo(job.Id, "r", 1, 1, false), run)], scans: 1)) reporter.Submit(m);
        Assert.AreEqual(0, reporter.Invalid.Count, string.Join("; ", reporter.Invalid));
    }
}
