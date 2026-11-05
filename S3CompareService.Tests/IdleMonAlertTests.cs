using S3CompareService.Core.Models;
using S3CompareService.Core.Notifications;
using S3CompareService.Worker.IdleMon;
using static S3CompareService.Tests.IdleMonTestData;

namespace S3CompareService.Tests;

[TestClass]
public class IdleMonAlertTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);

    private static AlertEvaluation Evaluate(IReadOnlyList<JobState> states, IdleMonOptions? options = null, ScanProgressTracker? progress = null, DateTimeOffset? now = null) =>
        AlertEvaluator.Evaluate(states, now ?? Now, options ?? Options(), progress ?? new ScanProgressTracker());

    private static AlertCondition? Find(AlertEvaluation e, string key) => e.Conditions.SingleOrDefault(c => c.Key == key);

    // ---- run-failed ----

    [TestMethod]
    public void RunFailed_FiresWhenNewestFinishedRunFailed_AndResolvesWhenNextRunCompletes()
    {
        var job = Job();
        var machine = new AlertStateMachine();
        var failed = Run(job.Id, RunStatus.Failed, Now.UtcDateTime.AddMinutes(-30), null, r => r.ErrorMessage = "AccessDenied: the key cannot list the bucket");

        var firing = machine.Apply(Evaluate([State(job, null, failed)]).Conditions, Now);
        var alert = firing.Single();
        Assert.AreEqual("firing", alert.Status);
        Assert.AreEqual("run-failed/job1", alert.Key);
        Assert.AreEqual("error", alert.Severity);
        Assert.AreEqual("Comparison Archive vs Wasabi failed", alert.Title);
        Assert.AreEqual("AccessDenied: the key cannot list the bucket", alert.Message);

        var completed = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-5));
        var resolved = machine.Apply(Evaluate([State(job, null, failed, completed)]).Conditions, Now.AddMinutes(1));
        Assert.AreEqual(1, resolved.Count);
        Assert.AreEqual("resolved", resolved[0].Status);
        Assert.AreEqual("run-failed/job1", resolved[0].Key);
        Assert.IsNull(resolved[0].Severity);
        Assert.AreEqual(0, machine.FiringKeys.Count);
    }

    [TestMethod]
    public void RunFailed_MessageIsCappedAt1024Characters_AndCancelledRunsDoNotCount()
    {
        var job = Job();
        var failed = Run(job.Id, RunStatus.Failed, Now.UtcDateTime.AddMinutes(-30), null, r => r.ErrorMessage = new string('e', 5000));
        Assert.AreEqual(1024, Find(Evaluate([State(job, null, failed)]), "run-failed/job1")!.Message!.Length);

        // The newest FINISHED run decides; a later cancelled run does not hide the failure, a later completed run clears it.
        var cancelled = Run(job.Id, RunStatus.Cancelled, Now.UtcDateTime.AddMinutes(-10));
        Assert.IsNotNull(Find(Evaluate([State(job, null, failed, cancelled)]), "run-failed/job1"));
    }

    // ---- disparities ----

    [TestMethod]
    public void Disparities_DefaultThresholdAlertsOnAnyConfirmedDisparity_AsWarning()
    {
        var job = Job();
        var clean = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-30));
        Assert.IsNull(Find(Evaluate([State(job, null, clean)]), "disparities/job1"));

        var dirty = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-30), null, r => { r.FilteredDifferencesCount = 12; r.SizeMismatchCount = 12; });
        var condition = Find(Evaluate([State(job, null, dirty)]), "disparities/job1")!;
        Assert.AreEqual("warning", condition.Severity);
        Assert.AreEqual("12 confirmed disparities in Archive vs Wasabi", condition.Title);
        Assert.AreEqual(12L, condition.Fields["confirmed"]);
    }

    [TestMethod]
    public void Disparities_BecomeAnError_WhenObjectsAreMissingInBucketB()
    {
        var job = Job();
        var run = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-30), null, r => { r.FilteredDifferencesCount = 3; r.MissingInBucketBCount = 1; r.SizeMismatchCount = 2; });
        Assert.AreEqual("error", Find(Evaluate([State(job, null, run)]), "disparities/job1")!.Severity);
    }

    [TestMethod]
    public void Disparities_FollowTheJobsOwnThresholds_LikeTheEmailAlerting()
    {
        var job = Job();
        job.AlertConfig.MaxDisparitiesThreshold = 100;
        var run = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-30), null, r => { r.FilteredDifferencesCount = 60; r.SizeMismatchCount = 60; });
        Assert.IsNull(Find(Evaluate([State(job, null, run)]), "disparities/job1"), "60 is not above 100");

        job.AlertConfig.MaxSizeMismatchThreshold = 50; // a per-type threshold is exceeded
        Assert.IsNotNull(Find(Evaluate([State(job, null, run)]), "disparities/job1"));

        job.AlertConfig.MaxSizeMismatchThreshold = 60; // strictly above only
        Assert.IsNull(Find(Evaluate([State(job, null, run)]), "disparities/job1"));

        job.AlertConfig.AlertOnDisparities = false;
        job.AlertConfig.MaxDisparitiesThreshold = 0;
        job.AlertConfig.MaxSizeMismatchThreshold = 1;
        Assert.IsNull(Find(Evaluate([State(job, null, run)]), "disparities/job1"), "the job opted out of disparity alerts");
    }

    [TestMethod]
    public void Disparities_FireWhetherOrNotEmailAlertingIsOn()
    {
        var run = Run("job1", RunStatus.Completed, Now.UtcDateTime.AddMinutes(-30), null, r => r.FilteredDifferencesCount = 4);
        foreach (var emailOn in new[] { false, true })
        {
            var job = Job();
            job.AlertConfig.IsEnabled = emailOn;
            Assert.IsNotNull(Find(Evaluate([State(job, null, run)]), "disparities/job1"), $"IsEnabled={emailOn}");
        }
    }

    [TestMethod]
    public async Task SharedThresholdCheck_GivesTheSameAnswerAsEvaluateAlertConditionAsync()
    {
        var service = new AlertNotificationService();
        var runs = new[]
        {
            Run("j", RunStatus.Completed, Now.UtcDateTime, null, r => { r.FilteredDifferencesCount = 5; r.MissingInBucketBCount = 5; }),
            Run("j", RunStatus.Completed, Now.UtcDateTime, null, r => { r.FilteredDifferencesCount = 0; }),
            Run("j", RunStatus.Completed, Now.UtcDateTime, null, r => { r.SequenceGapCount = 9; }),
        };
        foreach (var run in runs)
        {
            var job = Job();
            job.AlertConfig.IsEnabled = true;
            job.AlertConfig.MaxSequenceGapThreshold = 3;
            var (should, reasons) = await service.EvaluateAlertConditionAsync(job, run);
            var shared = AlertNotificationService.EvaluateDisparityThresholds(job.AlertConfig, run);
            CollectionAssert.AreEqual(reasons, shared);
            Assert.AreEqual(should, shared.Count > 0);
        }
    }

    // ---- overdue ----

    [TestMethod]
    public void Overdue_FiresAfterMultiplierTimesCadencePlus15Minutes()
    {
        var job = Job(intervalMinutes: 60); // limit: 2 x 60 + 15 = 135 min
        var t = Now.UtcDateTime;
        Assert.IsNull(Find(Evaluate([State(job, null, Run(job.Id, RunStatus.Completed, t.AddMinutes(-144)))]), "overdue/job1"), "ended 134 min ago");
        var overdue = Find(Evaluate([State(job, null, Run(job.Id, RunStatus.Completed, t.AddMinutes(-146)))]), "overdue/job1")!;
        Assert.AreEqual("warning", overdue.Severity);
        Assert.AreEqual("Archive vs Wasabi has not completed a comparison for 2 h 16 min", overdue.Title);
        Assert.AreEqual(1, Evaluate([State(job, null, Run(job.Id, RunStatus.Completed, t.AddMinutes(-146)))]).OverdueJobs);
    }

    [TestMethod]
    public void Overdue_UsesTheMultiplierSetting()
    {
        var job = Job(intervalMinutes: 60);
        var state = State(job, null, Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-150)));
        Assert.IsNotNull(Find(Evaluate([state]), "overdue/job1"));
        Assert.IsNull(Find(Evaluate([state], Options(o => o.OverdueMultiplier = 3)), "overdue/job1"), "3 x 60 + 15 = 195 min");
    }

    [TestMethod]
    public void Overdue_WithoutAnyCompletedRun_IsMeasuredFromJobCreation()
    {
        var created = Now.UtcDateTime.AddMinutes(-100);
        var young = Job(createdAt: created);
        Assert.IsNull(Find(Evaluate([State(young)]), "overdue/job1"));

        var old = Job(createdAt: Now.UtcDateTime.AddHours(-5));
        Assert.IsNotNull(Find(Evaluate([State(old)]), "overdue/job1"));

        // A failed run is not a completed one.
        Assert.IsNotNull(Find(Evaluate([State(old, null, Run(old.Id, RunStatus.Failed, Now.UtcDateTime.AddMinutes(-20)))]), "overdue/job1"));
    }

    [TestMethod]
    public void Overdue_NeverForManualOrDisabledJobs()
    {
        var manual = Job();
        manual.ScheduleType = JobScheduleType.Manual;
        manual.CreatedAtUtc = Now.UtcDateTime.AddDays(-30);
        Assert.IsNull(Find(Evaluate([State(manual)]), "overdue/job1"));

        var disabled = Job(enabled: false, createdAt: Now.UtcDateTime.AddDays(-30));
        Assert.AreEqual(0, Evaluate([State(disabled)]).Conditions.Count);
    }

    [TestMethod]
    public void Overdue_ForCronJobs_UsesTheGapBetweenOccurrences()
    {
        var job = Job(createdAt: Now.UtcDateTime.AddHours(-40));
        job.ScheduleType = JobScheduleType.CronExpression;
        job.CronExpression = "0 2 * * *"; // daily: limit 2 x 24 h + 15 min
        Assert.IsNull(Find(Evaluate([State(job)]), "overdue/job1"), "40 h is below 48 h 15 min");

        job.CreatedAtUtc = Now.UtcDateTime.AddHours(-50);
        Assert.IsNotNull(Find(Evaluate([State(job)]), "overdue/job1"), "50 h is above it");
    }

    [TestMethod]
    public void Overdue_ResolvesWhenARunCompletes()
    {
        var job = Job(createdAt: Now.UtcDateTime.AddHours(-5));
        var machine = new AlertStateMachine();
        Assert.AreEqual("firing", machine.Apply(Evaluate([State(job)]).Conditions, Now).Single().Status);

        var done = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-20));
        var after = machine.Apply(Evaluate([State(job, null, done)]).Conditions, Now.AddMinutes(5));
        Assert.AreEqual("resolved", after.Single().Status);
        Assert.AreEqual("overdue/job1", after.Single().Key);
    }

    // ---- scan-stuck ----

    [TestMethod]
    public void ScanStuck_FiresWhenObjectsComparedHasNotChangedForTheConfiguredMinutes()
    {
        var job = Job();
        var progress = new ScanProgressTracker();
        var t0 = Now;
        ActiveScanInfo Scan(long objects, bool paused = false) => new(job.Id, "run1", objects, 10, paused);

        progress.Observe([Scan(100)], t0);
        Assert.AreEqual(0, Evaluate([State(job, Scan(100))], progress: progress, now: t0).StuckScans.Count);

        progress.Observe([Scan(100)], t0.AddMinutes(14));
        Assert.AreEqual(0, Evaluate([State(job, Scan(100))], progress: progress, now: t0.AddMinutes(14)).StuckScans.Count, "14 min is below 15");

        progress.Observe([Scan(100)], t0.AddMinutes(16));
        var stuck = Evaluate([State(job, Scan(100))], progress: progress, now: t0.AddMinutes(16));
        Assert.AreEqual(1, stuck.StuckScans.Count);
        var condition = Find(stuck, "scan-stuck/job1")!;
        Assert.AreEqual("warning", condition.Severity);
        Assert.AreEqual("Scan of Archive vs Wasabi has made no progress for 16 min", condition.Title);

        // Progress resumes: the alert resolves.
        var machine = new AlertStateMachine();
        machine.Apply(stuck.Conditions, t0.AddMinutes(16));
        progress.Observe([Scan(150)], t0.AddMinutes(21));
        var resumed = Evaluate([State(job, Scan(150))], progress: progress, now: t0.AddMinutes(21));
        Assert.AreEqual(0, resumed.StuckScans.Count);
        Assert.AreEqual("resolved", machine.Apply(resumed.Conditions, t0.AddMinutes(21)).Single().Status);
    }

    [TestMethod]
    public void ScanStuck_IgnoresPausedScans_AndResolvesWhenTheScanEnds()
    {
        var job = Job();
        var progress = new ScanProgressTracker();
        ActiveScanInfo Scan(bool paused) => new(job.Id, "run1", 100, 0, paused);
        progress.Observe([Scan(false)], Now);
        progress.Observe([Scan(true)], Now.AddMinutes(30));
        Assert.AreEqual(0, Evaluate([State(job, Scan(true))], progress: progress, now: Now.AddMinutes(30)).StuckScans.Count);

        // Resumed after the pause: the clock starts again from the resume.
        progress.Observe([Scan(false)], Now.AddMinutes(35));
        Assert.AreEqual(0, Evaluate([State(job, Scan(false))], progress: progress, now: Now.AddMinutes(35)).StuckScans.Count);

        var machine = new AlertStateMachine();
        progress.Observe([Scan(false)], Now.AddMinutes(60));
        machine.Apply(Evaluate([State(job, Scan(false))], progress: progress, now: Now.AddMinutes(60)).Conditions, Now.AddMinutes(60));
        Assert.AreEqual(1, machine.FiringKeys.Count);

        progress.Observe([], Now.AddMinutes(65));
        var ended = machine.Apply(Evaluate([State(job)], progress: progress, now: Now.AddMinutes(65)).Conditions, Now.AddMinutes(65));
        Assert.IsTrue(ended.Any(m => m.Status == "resolved" && m.Key == "scan-stuck/job1"));
    }

    // ---- state machine ----

    [TestMethod]
    public void DisablingOrDeletingAJob_ResolvesAllOfItsAlerts()
    {
        var job = Job(createdAt: Now.UtcDateTime.AddHours(-5));
        var failed = Run(job.Id, RunStatus.Failed, Now.UtcDateTime.AddMinutes(-30), null, r => r.ErrorMessage = "x");
        var dirty = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddHours(-4), null, r => r.FilteredDifferencesCount = 3);
        var machine = new AlertStateMachine();

        var firing = machine.Apply(Evaluate([State(job, null, failed, dirty)]).Conditions, Now);
        CollectionAssert.AreEquivalent(new[] { "run-failed/job1", "disparities/job1", "overdue/job1" }, firing.Select(m => m.Key).ToArray());

        job.IsEnabled = false;
        var afterDisable = machine.Apply(Evaluate([State(job, null, failed, dirty)]).Conditions, Now.AddMinutes(5));
        Assert.AreEqual(3, afterDisable.Count);
        Assert.IsTrue(afterDisable.All(m => m.Status == "resolved"));
        Assert.AreEqual("job1", afterDisable[0].Tags!["jobId"], "resolved keeps the job tags");

        job.IsEnabled = true;
        machine.Apply(Evaluate([State(job, null, failed, dirty)]).Conditions, Now.AddMinutes(10));
        var afterDelete = machine.Apply(Evaluate([]).Conditions, Now.AddMinutes(15));
        Assert.AreEqual(3, afterDelete.Count(m => m.Status == "resolved"));
    }

    [TestMethod]
    public void FiringAlerts_AreRefreshedEveryPass_AndNothingIsResolvedThatWasNeverSent()
    {
        var job = Job();
        var failed = Run(job.Id, RunStatus.Failed, Now.UtcDateTime.AddMinutes(-30), null, r => r.ErrorMessage = "x");
        var machine = new AlertStateMachine();

        // First pass after a restart with a healthy job: nothing at all (no resolved spam).
        Assert.AreEqual(0, machine.Apply(Evaluate([State(job, null, Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddMinutes(-5)))]).Conditions, Now).Count);

        // A true condition is sent as firing on every pass.
        for (var i = 0; i < 3; i++)
        {
            var messages = machine.Apply(Evaluate([State(job, null, failed)]).Conditions, Now.AddMinutes(5 * i));
            Assert.AreEqual(1, messages.Count);
            Assert.AreEqual("firing", messages[0].Status);
        }
    }

    [TestMethod]
    public void DisabledJob_HasNoAlertsAtAll()
    {
        var job = Job(enabled: false, createdAt: Now.UtcDateTime.AddDays(-9));
        var failed = Run(job.Id, RunStatus.Failed, Now.UtcDateTime.AddMinutes(-30), null, r => r.ErrorMessage = "x");
        Assert.AreEqual(0, Evaluate([State(job, null, failed)]).Conditions.Count);
    }

    [TestMethod]
    public void AllAlertMessages_PassContractValidation()
    {
        var job = Job(createdAt: Now.UtcDateTime.AddHours(-5), name: new string('J', 300));
        var failed = Run(job.Id, RunStatus.Failed, Now.UtcDateTime.AddMinutes(-30), null, r => r.ErrorMessage = new string('e', 3000));
        var dirty = Run(job.Id, RunStatus.Completed, Now.UtcDateTime.AddHours(-4), null, r => { r.FilteredDifferencesCount = 3; r.MissingInBucketBCount = 3; });
        var reporter = new CapturingReporter(new FakeClock(Now));
        var machine = new AlertStateMachine();
        foreach (var m in machine.Apply(Evaluate([State(job, null, failed, dirty)]).Conditions, Now)) reporter.Submit(m);
        foreach (var m in machine.Apply([], Now.AddMinutes(5))) reporter.Submit(m);
        Assert.IsTrue(reporter.Messages.Count >= 6);
        Assert.AreEqual(0, reporter.Invalid.Count, string.Join("; ", reporter.Invalid));
        Assert.IsTrue(reporter.Messages.All(m => m.Title is null || m.Title.Length <= 200));
    }
}
