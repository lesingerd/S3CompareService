using S3CompareService.Worker.IdleMon;
using static S3CompareService.Tests.IdleMonTestData;

namespace S3CompareService.Tests;

[TestClass]
public class IdleMonHeartbeatTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);

    private static HeartbeatInput Healthy(Action<HeartbeatInputBuilder>? configure = null)
    {
        var b = new HeartbeatInputBuilder();
        configure?.Invoke(b);
        return b.Build();
    }

    /// <summary>A mutable stand-in for the init-only input.</summary>
    private sealed class HeartbeatInputBuilder
    {
        public DateTimeOffset? LastCollectionOk = Now.AddSeconds(-20);
        public string? DatabaseError;
        public DateTimeOffset? Scheduler = Now.AddSeconds(-5);
        public List<StuckScan> Stuck = [];
        public TimeSpan? Backlog;

        public HeartbeatInput Build() => new()
        {
            Now = Now, StartedAt = Now.AddHours(-1), LastCollectionOk = LastCollectionOk, DatabaseError = DatabaseError,
            SchedulerLastEvaluation = Scheduler, StuckScans = Stuck, Backlog = Backlog,
        };
    }

    [TestMethod]
    public void Healthy_IsOk_WithIntervalAndDataIntervalTag_AndNoBacklogTag()
    {
        var options = Options(o => { o.HeartbeatIntervalSeconds = 45; o.MetricsIntervalSeconds = 120; });
        var hb = HeartbeatBuilder.Build(Healthy(), options);
        Assert.AreEqual("heartbeat", hb.Kind);
        Assert.AreEqual("ok", hb.Status);
        Assert.IsNull(hb.Message);
        Assert.AreEqual(45, hb.IntervalSeconds);
        Assert.AreEqual("120", hb.Tags!["dataIntervalSeconds"]);
        Assert.IsFalse(hb.Tags.ContainsKey("backlogSeconds"));
    }

    [TestMethod]
    public void DatabaseUnreadable_IsDegraded_AndNamedFirst()
    {
        var hb = HeartbeatBuilder.Build(Healthy(b => { b.DatabaseError = "SqliteException"; b.Stuck.Add(new StuckScan("Archive", 20)); }), Options());
        Assert.AreEqual("degraded", hb.Status);
        Assert.AreEqual("Database cannot be read: SqliteException (+1 more)", hb.Message);
    }

    [TestMethod]
    public void SchedulerStalled_IsDegradedAfterFiveMinutes()
    {
        Assert.AreEqual("ok", HeartbeatBuilder.Build(Healthy(b => b.Scheduler = Now.AddMinutes(-4)), Options()).Status);
        var hb = HeartbeatBuilder.Build(Healthy(b => b.Scheduler = Now.AddMinutes(-6)), Options());
        Assert.AreEqual("degraded", hb.Status);
        Assert.AreEqual("Scheduler: no evaluation for 6 min", hb.Message);
    }

    [TestMethod]
    public void NoSchedulerInformation_DoesNotDegrade()
    {
        Assert.AreEqual("ok", HeartbeatBuilder.Build(Healthy(b => b.Scheduler = null), Options()).Status);
    }

    [TestMethod]
    public void StuckScan_IsDegraded_AndMoreProblemsAreCounted()
    {
        var one = HeartbeatBuilder.Build(Healthy(b => b.Stuck.Add(new StuckScan("Archive", 20))), Options());
        Assert.AreEqual("degraded", one.Status);
        Assert.AreEqual("Scan of Archive has made no progress for 20 min", one.Message);

        var many = HeartbeatBuilder.Build(Healthy(b =>
        {
            b.Scheduler = Now.AddMinutes(-9);
            b.Stuck.Add(new StuckScan("Archive", 20));
            b.Stuck.Add(new StuckScan("Backup", 30));
        }), Options());
        Assert.AreEqual("Scheduler: no evaluation for 9 min (+2 more)", many.Message);
    }

    [TestMethod]
    public void CollectorWithoutResult_IsDegraded_AfterThreeIntervalsOrTenMinutes()
    {
        var options = Options(o => o.MetricsIntervalSeconds = 300); // 3 x 300 s = 15 min
        Assert.AreEqual("ok", HeartbeatBuilder.Build(Healthy(b => b.LastCollectionOk = Now.AddMinutes(-14)), options).Status);
        var hb = HeartbeatBuilder.Build(Healthy(b => b.LastCollectionOk = Now.AddMinutes(-16)), options);
        Assert.AreEqual("degraded", hb.Status);
        Assert.AreEqual("Collector: no result for 16 min", hb.Message);

        var quick = Options(o => o.MetricsIntervalSeconds = 10); // 3 x 10 s is below the 10 minute minimum
        Assert.AreEqual("ok", HeartbeatBuilder.Build(Healthy(b => b.LastCollectionOk = Now.AddMinutes(-9)), quick).Status);
        Assert.AreEqual("degraded", HeartbeatBuilder.Build(Healthy(b => b.LastCollectionOk = Now.AddMinutes(-11)), quick).Status);

        // Never collected: counted from the service start.
        var never = HeartbeatBuilder.Build(Healthy(b => b.LastCollectionOk = null), options);
        Assert.AreEqual("degraded", never.Status);
    }

    [TestMethod]
    public void BacklogSeconds_IsWholeSeconds_AndOnlyPresentWhileTheSpoolHoldsMessages()
    {
        var hb = HeartbeatBuilder.Build(Healthy(b => b.Backlog = TimeSpan.FromSeconds(5400.9)), Options());
        Assert.AreEqual("5400", hb.Tags!["backlogSeconds"]);
        Assert.AreEqual("ok", hb.Status, "a backlog alone is not degraded; the hub shows it as catching up");
        Assert.IsFalse(HeartbeatBuilder.Build(Healthy(), Options()).Tags!.ContainsKey("backlogSeconds"));
    }

    [TestMethod]
    public void Stopping_HasStoppingStatus_AndIsValid()
    {
        var hb = HeartbeatBuilder.Stopping(Now, Options(), null);
        Assert.AreEqual("stopping", hb.Status);
        hb.Id = "x1";
        Assert.IsNull(IdleMonValidator.Validate(hb, Now));
    }

    [TestMethod]
    public void MessageIsCappedAt1024Characters()
    {
        var hb = HeartbeatBuilder.Build(Healthy(b => b.Stuck.Add(new StuckScan(new string('x', 2000), 20))), Options());
        Assert.AreEqual(1024, hb.Message!.Length);
        hb.Id = "x1";
        Assert.IsNull(IdleMonValidator.Validate(hb, Now));
    }
}
