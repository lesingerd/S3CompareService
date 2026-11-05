using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.IdleMon;
using S3CompareService.Worker.Services;
using static S3CompareService.Tests.IdleMonTestData;

namespace S3CompareService.Tests;

/// <summary>The collector against a real (temporary) SQLite database, a fake clock and a capturing reporter.</summary>
[TestClass]
public class IdleMonCollectorTests : TestDbBase
{
    private FakeClock _clock = null!;
    private CapturingReporter _reporter = null!;
    private FakeScanSource _scans = null!;
    private DateTimeOffset? _schedulerAt;

    [TestInitialize]
    public void Init()
    {
        _clock = new FakeClock();
        _reporter = new CapturingReporter(_clock);
        _scans = new FakeScanSource();
        _schedulerAt = _clock.GetUtcNow();
    }

    private IdleMonCollector NewCollector(IdleMonOptions? options = null, IJobRepository? jobs = null, IdleMonSpool? spool = null) =>
        new(options ?? Options(o => { o.MetricsIntervalSeconds = 10; o.HeartbeatIntervalSeconds = 10; }),
            jobs ?? JobRepo, RunRepo, DiffRepo, _scans, _reporter, spool, () => _schedulerAt, _clock, NullLogger<IdleMonCollector>.Instance);

    private async Task<(ComparisonJob Job, ScanRun Run)> SeedCompletedJobAsync(int confirmed = 3, int missingInB = 2)
    {
        var job = Job("job1");
        await JobRepo.SaveAsync(job);
        var start = _clock.UtcNow.AddMinutes(-20);
        var run = Run(job.Id, RunStatus.Completed, start, TimeSpan.FromMinutes(10), r =>
        {
            r.FilteredDifferencesCount = confirmed; r.MissingInBucketBCount = missingInB; r.TotalDifferences = confirmed;
        });
        await RunRepo.CreateAsync(run);
        await RunRepo.CompleteAsync(run);
        return (job, run);
    }

    [TestMethod]
    public async Task CollectAsync_ReadsTheDatabase_AndSendsAlertsMetricsAndTheJobsSnapshot()
    {
        var (job, run) = await SeedCompletedJobAsync();
        var collector = NewCollector();
        await collector.CollectAsync(CancellationToken.None);

        Assert.AreEqual(0, _reporter.Invalid.Count, string.Join("; ", _reporter.Invalid));
        Assert.AreEqual((double)run.ObjectsCompared, _reporter.Metrics("s3c.run.objects_compared").Single().Value);
        Assert.AreEqual(3.0, _reporter.Metrics("s3c.run.disparities.confirmed").Single().Value);
        Assert.AreEqual(1.0, _reporter.Metrics("s3c.jobs.enabled").Single().Value);
        Assert.AreEqual("error", _reporter.Alerts("disparities/job1").Single().Severity, "missing in bucket B makes it an error");

        var snapshot = _reporter.Messages.Single(m => m.Kind == "snapshot" && m.Name == "s3c.jobs");
        Assert.IsNull(snapshot.Tags, "the jobs snapshot has no job tag");
        Assert.AreEqual(15, snapshot.Columns!.Count);
        Assert.AreEqual(1, snapshot.Rows!.Count);
        var row = snapshot.Rows[0];
        Assert.AreEqual(job.Name, row[0]);
        Assert.AreEqual(true, row[1]);
        Assert.AreEqual("every 60 min", row[2]);
        Assert.AreEqual("contoso-archive", row[4]);
        Assert.AreEqual("Completed", row[7]);
        Assert.AreEqual(3L, row[9]);
        Assert.AreEqual(2L, row[11]);
        Assert.AreEqual(false, row[14]);
        Assert.IsFalse(_reporter.Messages.Any(m => m.Name == "s3c.run.top_disparities"), "object keys are off by default");
    }

    [TestMethod]
    public async Task IncludeBucketNamesFalse_SendsAAndBInTagsAndSnapshot()
    {
        await SeedCompletedJobAsync();
        await NewCollector(Options(o => o.IncludeBucketNames = false)).CollectAsync(CancellationToken.None);

        Assert.IsTrue(_reporter.Messages.Where(m => m.Tags is not null && m.Tags.ContainsKey("jobId")).All(m => m.Tags!["bucketA"] == "A" && m.Tags["bucketB"] == "B"));
        var row = _reporter.Messages.Single(m => m.Name == "s3c.jobs").Rows![0];
        Assert.AreEqual("A", row[4]);
        Assert.AreEqual("B", row[5]);
        Assert.IsFalse(_reporter.Messages.Any(m => System.Text.Json.JsonSerializer.Serialize(m.Tags).Contains("contoso")));
    }

    [TestMethod]
    public async Task IncludeObjectKeysFalse_NeverReadsOrSendsDifferences()
    {
        var (_, run) = await SeedCompletedJobAsync();
        await DiffRepo.InsertBatchAsync([new RecordedDifference { ScanRunId = run.Id, Key = "customers/acme/secret-file.pdf", Type = DifferenceType.MissingInBucketB, IsWithinGracePeriod = false }]);
        await NewCollector(Options(o => o.IncludeObjectKeys = false)).CollectAsync(CancellationToken.None);

        Assert.IsFalse(_reporter.Messages.Any(m => m.Name == "s3c.run.top_disparities"));
        Assert.IsFalse(_reporter.Messages.Any(m => System.Text.Json.JsonSerializer.Serialize(m).Contains("secret-file")), "no object key anywhere in what is sent");
    }

    [TestMethod]
    public async Task IncludeObjectKeysTrue_SendsUpTo50ConfirmedDifferences_WithTheRunEndTime()
    {
        var (job, run) = await SeedCompletedJobAsync(confirmed: 60);
        var diffs = Enumerable.Range(0, 60).Select(i => new RecordedDifference
        {
            ScanRunId = run.Id, Key = $"logs/{i:D3}.gz", Type = i % 2 == 0 ? DifferenceType.MissingInBucketB : DifferenceType.SizeMismatch,
            BucketASize = 100 + i, BucketBSize = i % 2 == 0 ? null : 90, BucketALastModified = _clock.UtcNow.AddHours(-2), IsWithinGracePeriod = false,
        }).Append(new RecordedDifference { ScanRunId = run.Id, Key = "in-flight.gz", Type = DifferenceType.MissingInBucketB, IsWithinGracePeriod = true }).ToList();
        await DiffRepo.InsertBatchAsync(diffs);

        await NewCollector(Options(o => o.IncludeObjectKeys = true)).CollectAsync(CancellationToken.None);

        var snapshot = _reporter.Messages.Single(m => m.Name == "s3c.run.top_disparities");
        Assert.AreEqual(0, _reporter.Invalid.Count, string.Join("; ", _reporter.Invalid));
        Assert.AreEqual(50, snapshot.Rows!.Count);
        CollectionAssert.AreEqual(new[] { "key", "type", "sizeA", "sizeB", "lastModifiedA", "lastModifiedB" }, snapshot.Columns!.Select(c => c.Name).ToArray());
        Assert.IsFalse(snapshot.Rows.Any(r => (string?)r[0] == "in-flight.gz"), "in-flight replication is not a confirmed disparity");
        Assert.AreEqual(job.Id, snapshot.Tags!["jobId"]);
        Assert.AreEqual(IdleMonWire.Timestamp(new DateTimeOffset(run.EndTimeUtc!.Value, TimeSpan.Zero)), snapshot.Timestamp);
    }

    [TestMethod]
    public async Task UnreadableDatabase_SendsNothing_KeepsAlertState_AndMakesTheHeartbeatDegraded()
    {
        await SeedCompletedJobAsync();
        var collector = NewCollector(jobs: new FailingJobRepository(failAfter: 1, inner: JobRepo));
        await collector.TickAsync(CancellationToken.None); // works: collection + heartbeat
        Assert.AreEqual("ok", _reporter.LastHeartbeat!.Status);
        Assert.IsTrue(collector.FiringAlerts.Contains("disparities/job1"));
        var before = _reporter.Messages.Count;

        _clock.Advance(TimeSpan.FromSeconds(10));
        await collector.TickAsync(CancellationToken.None); // the database cannot be read now
        var newMessages = _reporter.Messages.Skip(before).ToList();
        Assert.AreEqual(1, newMessages.Count, "only the heartbeat: unknown is not zero, so no metrics, no alerts, no resolved");
        Assert.AreEqual("degraded", newMessages[0].Status);
        StringAssert.StartsWith(newMessages[0].Message, "Database cannot be read");
        Assert.IsTrue(collector.FiringAlerts.Contains("disparities/job1"), "alert state is untouched");
    }

    [TestMethod]
    public async Task TickAsync_FollowsTheTwoIntervals_AndTheHeartbeatComesAfterTheCollection()
    {
        await SeedCompletedJobAsync();
        var collector = NewCollector(Options(o => { o.MetricsIntervalSeconds = 30; o.HeartbeatIntervalSeconds = 10; }));

        await collector.TickAsync(CancellationToken.None);
        Assert.AreEqual(1, _reporter.Messages.Count(m => m.Name == "s3c.jobs"));
        Assert.AreEqual(1, _reporter.Messages.Count(m => m.Kind == "heartbeat"));
        Assert.AreEqual("heartbeat", _reporter.Messages.Last().Kind);

        _clock.Advance(TimeSpan.FromSeconds(10));
        await collector.TickAsync(CancellationToken.None);
        Assert.AreEqual(1, _reporter.Messages.Count(m => m.Name == "s3c.jobs"), "not due yet");
        Assert.AreEqual(2, _reporter.Messages.Count(m => m.Kind == "heartbeat"));

        _clock.Advance(TimeSpan.FromSeconds(20));
        await collector.TickAsync(CancellationToken.None);
        Assert.AreEqual(2, _reporter.Messages.Count(m => m.Name == "s3c.jobs"));
        Assert.AreEqual(3, _reporter.Messages.Count(m => m.Kind == "heartbeat"));
        Assert.AreEqual(0, _reporter.Invalid.Count, string.Join("; ", _reporter.Invalid));
    }

    [TestMethod]
    public async Task Heartbeat_ReportsBacklogOfTheSpool_AndDegradesWhenTheSchedulerStops()
    {
        await SeedCompletedJobAsync();
        var dir = Path.Combine(Path.GetTempPath(), $"idlemon_hb_{Guid.NewGuid():N}");
        try
        {
            var spool = new IdleMonSpool(dir, 1_000_000, _clock);
            var old = IdleMonMessage.Metric(_clock.GetUtcNow().AddMinutes(-90), "s3c.old", 1, "count");
            old.Id = "old1";
            spool.Write([IdleMonWire.BuildEnvelope("FS01", "1.0.0", "default", _clock.GetUtcNow(), [IdleMonWire.Serialize(old)])]);

            var collector = NewCollector(spool: spool);
            await collector.TickAsync(CancellationToken.None);
            var hb = _reporter.LastHeartbeat!;
            Assert.AreEqual("ok", hb.Status);
            Assert.AreEqual((90 * 60).ToString(), hb.Tags!["backlogSeconds"]);
            Assert.AreEqual("10", hb.Tags["dataIntervalSeconds"]);

            _clock.Advance(TimeSpan.FromMinutes(6)); // the scheduler loop did not record an evaluation meanwhile
            await collector.TickAsync(CancellationToken.None);
            Assert.AreEqual("degraded", _reporter.LastHeartbeat!.Status);
            StringAssert.StartsWith(_reporter.LastHeartbeat.Message, "Scheduler: no evaluation");
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    [TestMethod]
    public async Task StuckScan_MakesTheHeartbeatDegraded_AndFiresTheAlert()
    {
        await SeedCompletedJobAsync(confirmed: 0, missingInB: 0);
        _scans.Scans.Add(new ActiveScanInfo("job1", "run-live", 500, 5, false));
        var collector = NewCollector(Options(o => { o.MetricsIntervalSeconds = 300; o.HeartbeatIntervalSeconds = 60; o.StuckScanMinutes = 15; }));

        await collector.TickAsync(CancellationToken.None);
        Assert.AreEqual("ok", _reporter.LastHeartbeat!.Status);
        Assert.AreEqual(500.0, _reporter.Metrics("s3c.scan.objects_compared").Single().Value);

        for (var i = 0; i < 4; i++)
        {
            _schedulerAt = _clock.GetUtcNow();
            _clock.Advance(TimeSpan.FromMinutes(5));
            _schedulerAt = _clock.GetUtcNow();
            await collector.TickAsync(CancellationToken.None);
        }

        Assert.IsTrue(collector.FiringAlerts.Contains("scan-stuck/job1"));
        Assert.AreEqual("degraded", _reporter.LastHeartbeat!.Status);
        StringAssert.Contains(_reporter.LastHeartbeat.Message, "has made no progress");
    }

    [TestMethod]
    public async Task ResultMetrics_StopAfterTheWindow_WhileJobMetricsContinue()
    {
        await SeedCompletedJobAsync(confirmed: 0, missingInB: 0); // ended 10 minutes ago, interval 60 min: window 180 min
        var collector = NewCollector();
        await collector.CollectAsync(CancellationToken.None);
        Assert.AreEqual(1, _reporter.Metrics("s3c.run.objects_compared").Count());

        _clock.Advance(TimeSpan.FromMinutes(100));
        await collector.CollectAsync(CancellationToken.None);
        Assert.AreEqual(2, _reporter.Metrics("s3c.run.objects_compared").Count());

        _clock.Advance(TimeSpan.FromMinutes(100)); // 210 minutes after the run ended
        await collector.CollectAsync(CancellationToken.None);
        Assert.AreEqual(2, _reporter.Metrics("s3c.run.objects_compared").Count(), "no longer re-sent");
        Assert.AreEqual(3, _reporter.Metrics("s3c.job.consecutive_failures").Count());
        Assert.AreEqual(3, _reporter.Metrics("s3c.job.last_success.age").Count());
    }

    [TestMethod]
    public async Task EmptyDatabase_SendsServiceMetrics_AndAnEmptyJobsSnapshot()
    {
        await NewCollector().CollectAsync(CancellationToken.None);
        Assert.AreEqual(0, _reporter.Invalid.Count, string.Join("; ", _reporter.Invalid));
        Assert.AreEqual(0.0, _reporter.Metrics("s3c.jobs.enabled").Single().Value);
        Assert.AreEqual(0, _reporter.Messages.Single(m => m.Name == "s3c.jobs").Rows!.Count);
    }

    [TestMethod]
    public async Task JobsSnapshot_HoldsAtMost100Rows()
    {
        for (var i = 0; i < 105; i++) await JobRepo.SaveAsync(Job($"job{i:D3}", $"Job {i:D3}", enabled: i % 2 == 0));
        await NewCollector().CollectAsync(CancellationToken.None);
        var snapshot = _reporter.Messages.Single(m => m.Name == "s3c.jobs");
        Assert.AreEqual(100, snapshot.Rows!.Count);
        Assert.AreEqual(0, _reporter.Invalid.Count, string.Join("; ", _reporter.Invalid));
        Assert.IsTrue(snapshot.Rows.Take(53).All(r => (bool)r[1]!), "enabled jobs come first");
    }

    // ---- events never affect the service ----

    [TestMethod]
    public async Task Events_NeverThrow_EvenWhenTheReporterIsBroken()
    {
        var events = new IdleMonEvents(new ThrowingReporter(), Options(), _clock, "1.0.0");
        var job = Job();
        var run = Run(job.Id, RunStatus.Completed, _clock.UtcNow.AddMinutes(-5));
        events.RunCompleted(job, run);
        events.RunFailed(job, run, new Exception("x"));
        events.RunCancelled(job, run);
        events.RunRecovered(run, true);
        events.JobChanged("created", job);
        events.ServiceStarted(null);
        await Task.CompletedTask;
    }

    [TestMethod]
    public void Events_HaveTheCatalogCategoriesSeveritiesAndTexts()
    {
        var events = new IdleMonEvents(_reporter, Options(), _clock, "1.2.3");
        var job = Job();
        var run = Run(job.Id, RunStatus.Completed, _clock.UtcNow.AddHours(-3), TimeSpan.FromSeconds(3725), r => { r.ObjectsCompared = 120000; r.FilteredDifferencesCount = 3; r.InFlightDifferencesCount = 9; });
        events.RunCompleted(job, run);
        events.RunFailed(job, Run(job.Id, RunStatus.Failed, _clock.UtcNow, null, r => r.ErrorMessage = "AccessDenied"), new InvalidOperationException("ignored"));
        events.RunCancelled(job, run);
        events.RunRecovered(run, true);
        events.RunRecovered(run, false);
        events.JobChanged("deleted", job);
        events.ServiceStarted(4);
        events.ServiceStarted(null);

        Assert.AreEqual(0, _reporter.Invalid.Count, string.Join("; ", _reporter.Invalid));
        var m = _reporter.Messages;
        Assert.AreEqual("Comparison Archive vs Wasabi completed: 120,000 objects, 3 confirmed disparities, 9 in flight, 01:02:05", m[0].Message);
        Assert.AreEqual(("info", "scan"), (m[0].Severity, m[0].Category));
        Assert.AreEqual(3L, m[0].Fields!["confirmed"]);
        Assert.AreEqual(("error", "scan"), (m[1].Severity, m[1].Category));
        StringAssert.Contains(m[1].Message, "AccessDenied");
        Assert.AreEqual(("warning", "scan"), (m[2].Severity, m[2].Category));
        Assert.AreEqual(("warning", "recovery"), (m[3].Severity, m[3].Category));
        Assert.AreEqual(("warning", "recovery"), (m[4].Severity, m[4].Category));
        Assert.AreEqual(("info", "config"), (m[5].Severity, m[5].Category));
        Assert.AreEqual("Job Archive vs Wasabi deleted", m[5].Message);
        Assert.AreEqual(2, m[5].Tags!.Count, "config events carry names only");
        Assert.AreEqual(("info", "startup"), (m[6].Severity, m[6].Category));
        StringAssert.Contains(m[6].Message, "4 job(s)");
        Assert.IsFalse(m[7].Message!.Contains("job(s)"), "unknown job count is left out, not sent as 0");
    }

    [TestMethod]
    public async Task CrashRecovery_NeverFails_WhenIdleMonEventsThrow_AndReportsTheRecovery()
    {
        var job = Job("job1", enabled: false);
        await JobRepo.SaveAsync(job);
        var crashed = Run(job.Id, RunStatus.Running, _clock.UtcNow.AddMinutes(-30), null, r => { r.EndTimeUtc = null; r.ObjectsCompared = 50; });
        await RunRepo.CreateAsync(crashed);

        var alerts = new S3CompareService.Core.Notifications.AlertNotificationService();
        var coordinator = new ScanExecutionCoordinator(JobRepo, RunRepo, DiffRepo, AuditRepo, alerts, NullLogger<ScanExecutionCoordinator>.Instance, new ThrowingEvents());
        await new CrashRecoveryManager(RunRepo, JobRepo, AuditRepo, coordinator, NullLogger<CrashRecoveryManager>.Instance, new ThrowingEvents()).RunStartupRecoveryAsync();
        Assert.AreEqual(RunStatus.Interrupted, (await RunRepo.GetByIdAsync(crashed.Id))!.Status);

        var events = new IdleMonEvents(_reporter, Options(), _clock, "1.0.0");
        var coordinator2 = new ScanExecutionCoordinator(JobRepo, RunRepo, DiffRepo, AuditRepo, alerts, NullLogger<ScanExecutionCoordinator>.Instance, events);
        await RunRepo.CreateAsync(Run(job.Id, RunStatus.Running, _clock.UtcNow.AddMinutes(-20), null, r => { r.Id = "second"; r.EndTimeUtc = null; }));
        await new CrashRecoveryManager(RunRepo, JobRepo, AuditRepo, coordinator2, NullLogger<CrashRecoveryManager>.Instance, events).RunStartupRecoveryAsync();
        var recovery = _reporter.Messages.Where(x => x.Category == "recovery").ToList(); // the first run was already Interrupted: announced once, not at every start
        Assert.AreEqual(1, recovery.Count);
        Assert.AreEqual("warning", recovery[0].Severity);
    }

    /// <summary>Delegates to the real repository for the first calls, then throws like a locked or corrupt database.</summary>
    private sealed class FailingJobRepository(int failAfter, IJobRepository inner) : IJobRepository
    {
        private int _calls;

        private void Check()
        {
            if (++_calls > failAfter) throw new Microsoft.Data.Sqlite.SqliteException("database is locked", 5);
        }

        public Task<List<ComparisonJob>> GetAllAsync(CancellationToken cancellationToken = default) { Check(); return inner.GetAllAsync(cancellationToken); }
        public Task<ComparisonJob?> GetByIdAsync(string id, CancellationToken cancellationToken = default) { Check(); return inner.GetByIdAsync(id, cancellationToken); }
        public Task SaveAsync(ComparisonJob job, CancellationToken cancellationToken = default) => inner.SaveAsync(job, cancellationToken);
        public Task DeleteAsync(string id, CancellationToken cancellationToken = default) => inner.DeleteAsync(id, cancellationToken);
        public Task UpdateRunScheduleStateAsync(string id, DateTime lastRunTimeUtc, DateTime? nextRunTimeUtc, RunStatus status, CancellationToken cancellationToken = default) => inner.UpdateRunScheduleStateAsync(id, lastRunTimeUtc, nextRunTimeUtc, status, cancellationToken);
        public Task UpdateNextRunTimeAsync(string id, DateTime? nextRunTimeUtc, CancellationToken cancellationToken = default) => inner.UpdateNextRunTimeAsync(id, nextRunTimeUtc, cancellationToken);
        public Task UpdateAlertTimestampsAsync(string id, DateTime? lastAlertSentUtc, DateTime? lastDailyDigestSentUtc, CancellationToken cancellationToken = default) => inner.UpdateAlertTimestampsAsync(id, lastAlertSentUtc, lastDailyDigestSentUtc, cancellationToken);
    }
}
