using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.IdleMon;
using S3CompareService.Worker.Services;
using static S3CompareService.Tests.IdleMonTestData;

namespace S3CompareService.Tests;

/// <summary>The heartbeat does not wait for the database (S-3) and "next due" uses the monotonic clock (S-7).</summary>
[TestClass]
public class IdleMonHeartbeatIsolationTests : TestDbBase
{
    private FakeClock _clock = null!;
    private CapturingReporter _reporter = null!;
    private SchedulerHeartbeat _scheduler = null!;

    [TestInitialize]
    public void Init()
    {
        _clock = new FakeClock();
        _reporter = new CapturingReporter(_clock);
        _scheduler = new SchedulerHeartbeat(_clock);
    }

    private IdleMonCollector NewCollector(IdleMonOptions options, IJobRepository jobs, TimeProvider? time = null, IIdleMonReporter? reporter = null) =>
        new(options, jobs, RunRepo, DiffRepo, new FakeScanSource(), reporter ?? _reporter, null, () => _scheduler.LastEvaluation, time ?? _clock, NullLogger<IdleMonCollector>.Instance);

    /// <summary>A job repository whose reads wait (like a locked database) after <paramref name="freeCalls"/> calls, until released.</summary>
    private sealed class BlockingJobRepository(IJobRepository inner, int freeCalls = 0) : IJobRepository
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _calls;

        public int BlockedCalls => Volatile.Read(ref _calls) - freeCalls;

        public void Release() => _gate.TrySetResult();

        public async Task<List<ComparisonJob>> GetAllAsync(CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _calls) > freeCalls) await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return await inner.GetAllAsync(cancellationToken).ConfigureAwait(false);
        }

        public Task<ComparisonJob?> GetByIdAsync(string id, CancellationToken cancellationToken = default) => inner.GetByIdAsync(id, cancellationToken);
        public Task SaveAsync(ComparisonJob job, CancellationToken cancellationToken = default) => inner.SaveAsync(job, cancellationToken);
        public Task DeleteAsync(string id, CancellationToken cancellationToken = default) => inner.DeleteAsync(id, cancellationToken);
        public Task UpdateRunScheduleStateAsync(string id, DateTime lastRunTimeUtc, DateTime? nextRunTimeUtc, RunStatus status, CancellationToken cancellationToken = default) => inner.UpdateRunScheduleStateAsync(id, lastRunTimeUtc, nextRunTimeUtc, status, cancellationToken);
        public Task UpdateNextRunTimeAsync(string id, DateTime? nextRunTimeUtc, CancellationToken cancellationToken = default) => inner.UpdateNextRunTimeAsync(id, nextRunTimeUtc, cancellationToken);
        public Task UpdateAlertTimestampsAsync(string id, DateTime? lastAlertSentUtc, DateTime? lastDailyDigestSentUtc, CancellationToken cancellationToken = default) => inner.UpdateAlertTimestampsAsync(id, lastAlertSentUtc, lastDailyDigestSentUtc, cancellationToken);
    }

    [TestMethod]
    public async Task HeartbeatCadenceContinues_WhileTheCollectionIsBlocked_AndTurnsDegradedWhenItStaysBlocked()
    {
        var jobs = new BlockingJobRepository(JobRepo);
        var collector = NewCollector(Options(o => { o.MetricsIntervalSeconds = 10; o.HeartbeatIntervalSeconds = 10; }), jobs);

        var collecting = collector.CollectIfDueAsync(CancellationToken.None); // blocks inside the "database"
        Assert.IsFalse(collecting.IsCompleted, "the pass is stuck");

        for (var i = 0; i < 5; i++)
        {
            _clock.Advance(TimeSpan.FromSeconds(10));
            _scheduler.Record();
            Assert.IsTrue(collector.HeartbeatIfDue(), $"heartbeat {i} is sent although the collection never returns");
        }

        Assert.AreEqual(5, _reporter.Messages.Count(m => m.Kind == "heartbeat"));
        Assert.IsTrue(_reporter.Messages.Where(m => m.Kind == "heartbeat").All(m => m.Status == "ok"), "a few minutes without a result is not yet a problem");

        _clock.Advance(TimeSpan.FromMinutes(11));
        _scheduler.Record();
        Assert.IsTrue(collector.HeartbeatIfDue());
        Assert.AreEqual("degraded", _reporter.LastHeartbeat!.Status);
        StringAssert.StartsWith(_reporter.LastHeartbeat.Message, "Collector: no result for");

        jobs.Release();
        await collecting;
        _clock.Advance(TimeSpan.FromSeconds(10));
        _scheduler.Record();
        collector.HeartbeatIfDue();
        Assert.AreEqual("ok", _reporter.LastHeartbeat!.Status, "healthy again once a pass completes");
        Assert.AreEqual(0, _reporter.Invalid.Count, string.Join("; ", _reporter.Invalid));
    }

    [TestMethod]
    public async Task TheHostedService_KeepsHeartbeating_OnItsOwnTask_WhileTheDatabaseIsBlocked()
    {
        var jobs = new BlockingJobRepository(JobRepo, freeCalls: 1); // the startup read works, every pass after it hangs
        var reporter = new CapturingReporter(TimeProvider.System);
        var collector = NewCollector(Options(o => { o.MetricsIntervalSeconds = 5; o.HeartbeatIntervalSeconds = 1; }), jobs, TimeProvider.System, reporter);
        var service = new IdleMonReportingService(collector, NoOpIdleMonEvents.Instance, jobs, TimeProvider.System, NullLogger<IdleMonReportingService>.Instance);

        await service.StartAsync(CancellationToken.None);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(3.3));
            var heartbeats = reporter.Messages.Count(m => m.Kind == "heartbeat");
            Assert.IsTrue(heartbeats >= 3, $"{heartbeats} heartbeats in 3.3 s with a 1 s interval");
            Assert.IsTrue(jobs.BlockedCalls >= 1, "the collection did start and is stuck");
            Assert.IsFalse(reporter.Messages.Any(m => m.Kind == "metric"), "no metrics: the pass never finished");
        }
        finally
        {
            jobs.Release();
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await service.StopAsync(stop.Token);
        }
    }

    [TestMethod]
    public async Task WallClockJumps_NeitherStallNorTriggerExtraRuns()
    {
        var collector = NewCollector(Options(o => { o.MetricsIntervalSeconds = 30; o.HeartbeatIntervalSeconds = 10; }), JobRepo);
        await collector.TickAsync(CancellationToken.None);
        Assert.AreEqual(1, _reporter.Messages.Count(m => m.Kind == "heartbeat"));
        Assert.AreEqual(1, _reporter.Messages.Count(m => m.Name == "s3c.jobs"));

        // The wall clock is set back five hours: with wall-clock "next due" nothing would run for five hours.
        _clock.StepWallClock(TimeSpan.FromHours(-5));
        await collector.TickAsync(CancellationToken.None);
        Assert.AreEqual(1, _reporter.Messages.Count(m => m.Kind == "heartbeat"), "no real time has passed: not due");
        Assert.IsTrue(collector.DelayUntilNextTick() <= TimeSpan.FromSeconds(10), "the delay is clamped to the interval");
        Assert.IsTrue(collector.DelayUntilNextTick() >= TimeSpan.FromMilliseconds(250));

        _clock.Advance(TimeSpan.FromSeconds(10));
        _scheduler.Record();
        await collector.TickAsync(CancellationToken.None);
        Assert.AreEqual(2, _reporter.Messages.Count(m => m.Kind == "heartbeat"), "ten real seconds later the heartbeat is due, whatever the wall clock says");
        Assert.AreEqual("ok", _reporter.LastHeartbeat!.Status, "no false 'no result for hours' from the jump");

        // The wall clock jumps forward a month: no burst of catch-up runs either.
        _clock.StepWallClock(TimeSpan.FromDays(30));
        await collector.TickAsync(CancellationToken.None);
        Assert.AreEqual(2, _reporter.Messages.Count(m => m.Kind == "heartbeat"));
        Assert.AreEqual(1, _reporter.Messages.Count(m => m.Name == "s3c.jobs"), "the 30 s metrics interval has not passed in real time");
        _clock.Advance(TimeSpan.FromSeconds(20));
        _scheduler.Record();
        await collector.TickAsync(CancellationToken.None);
        Assert.AreEqual(2, _reporter.Messages.Count(m => m.Name == "s3c.jobs"));
        Assert.AreEqual("ok", _reporter.LastHeartbeat!.Status);
        Assert.AreEqual(0, _reporter.Invalid.Count, string.Join("; ", _reporter.Invalid));
    }

    [TestMethod]
    public void TheDelayUntilTheNextTick_IsNeverLongerThanTheInterval_AndAtLeast250Ms()
    {
        var collector = NewCollector(Options(o => { o.MetricsIntervalSeconds = 300; o.HeartbeatIntervalSeconds = 60; }), JobRepo);
        Assert.AreEqual(TimeSpan.FromMilliseconds(250), collector.DelayUntilNextTick(), "nothing has run yet: due now");

        collector.HeartbeatIfDue();
        Assert.IsTrue(collector.DelayUntilNextHeartbeat() > TimeSpan.FromSeconds(59) && collector.DelayUntilNextHeartbeat() <= TimeSpan.FromSeconds(60));
        _clock.Advance(TimeSpan.FromSeconds(59.9));
        Assert.AreEqual(TimeSpan.FromMilliseconds(250), collector.DelayUntilNextHeartbeat(), "a tiny remainder is raised to 250 ms");
        _clock.Advance(TimeSpan.FromHours(10));
        Assert.AreEqual(TimeSpan.FromMilliseconds(250), collector.DelayUntilNextHeartbeat(), "overdue: run again at once");
    }

    [TestMethod]
    public void SchedulerHeartbeat_MeasuresItsAgeOnTheMonotonicClock()
    {
        var heartbeat = new SchedulerHeartbeat(_clock);
        heartbeat.Record();
        _clock.StepWallClock(TimeSpan.FromHours(-3));
        Assert.IsTrue(_clock.GetUtcNow() - heartbeat.LastEvaluation < TimeSpan.FromSeconds(1), "a clock set back does not make the scheduler look hours old, or in the future");
        _clock.StepWallClock(TimeSpan.FromHours(6));
        Assert.IsTrue(_clock.GetUtcNow() - heartbeat.LastEvaluation < TimeSpan.FromSeconds(1));

        _clock.Advance(TimeSpan.FromMinutes(6));
        Assert.AreEqual(6, (int)(_clock.GetUtcNow() - heartbeat.LastEvaluation).TotalMinutes, "real time passing is seen");
    }

    [TestMethod]
    public void AJobWhoseLastRunIsInTheFuture_BecauseTheClockWentBackwards_IsDue_NotStalled()
    {
        var now = new DateTime(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);
        var job = Job(intervalMinutes: 60);
        job.LastRunTimeUtc = now.AddHours(3); // recorded before the clock was set back three hours

        Assert.IsTrue(ScanSchedulerService.ComputeSchedule(job, now).IsDue);
        Assert.IsFalse(ScanSchedulerService.ComputeSchedule(job, now, now.AddMinutes(-10)).IsDue, "a normal anchor still waits for its interval");
        Assert.IsFalse(ScanSchedulerService.ComputeSchedule(job, now, now.AddMinutes(2)).IsDue, "a small skew is tolerated");

        var cron = Job();
        cron.ScheduleType = JobScheduleType.CronExpression;
        cron.CronExpression = "0 * * * *";
        cron.LastRunTimeUtc = now.AddHours(3);
        var (isDue, next) = ScanSchedulerService.ComputeSchedule(cron, now);
        Assert.IsFalse(isDue);
        Assert.AreEqual(new DateTime(2026, 11, 5, 13, 0, 0, DateTimeKind.Utc), next, "the next slot after now, not three hours later");
    }
}
