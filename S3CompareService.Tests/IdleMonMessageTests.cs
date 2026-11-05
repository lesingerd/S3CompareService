using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Worker.IdleMon;

namespace S3CompareService.Tests;

[TestClass]
public class IdleMonMessageTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);

    private static IdleMonMessage WithId(IdleMonMessage m)
    {
        m.Id = Guid.NewGuid().ToString("N");
        return m;
    }

    [TestMethod]
    public void Serialized_Message_PutsKindFirst_AndUsesMillisecondUtcTimestamps()
    {
        var all = new[]
        {
            IdleMonMessage.Event(Now, "info", "scan", "done"),
            IdleMonMessage.AlertFiring(Now, "run-failed/j1", "error", "failed", "why"),
            IdleMonMessage.AlertResolved(Now, "run-failed/j1"),
            IdleMonMessage.Metric(Now, "s3c.jobs.enabled", 3, "count"),
            IdleMonMessage.Heartbeat(Now, 60, "ok", null),
            IdleMonMessage.Snapshot(Now, "s3c.jobs", [new IdleMonColumn { Name = "job", Type = "string" }], [["a"]]),
        };

        foreach (var message in all)
        {
            WithId(message);
            Assert.IsNull(IdleMonValidator.Validate(message, Now), message.Kind);
            var json = Encoding.UTF8.GetString(IdleMonWire.Serialize(message));
            Assert.IsTrue(json.StartsWith($"{{\"kind\":\"{message.Kind}\""), json);
            Assert.IsTrue(Regex.IsMatch(json, "\"timestamp\":\"2026-11-05T12:00:00\\.000Z\""), json);
        }
    }

    [TestMethod]
    public void Timestamp_IsUtcWithMillisecondsAndZ_EvenForLocalOffsets()
    {
        var local = new DateTimeOffset(2026, 11, 5, 5, 30, 15, 123, TimeSpan.FromHours(-8));
        Assert.AreEqual("2026-11-05T13:30:15.123Z", IdleMonWire.Timestamp(local));
    }

    [TestMethod]
    public void Validator_EnforcesNameIdTimestampAndTagRules()
    {
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "S3C.Bad", 1, "count")), Now), "upper case name");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "s3c..bad", 1, "count")), Now), "double dot");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "s3c.ok", double.NaN, "count")), Now), "NaN");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "s3c.ok", 1, "furlongs")), Now), "unit");
        Assert.IsNotNull(IdleMonValidator.Validate(IdleMonMessage.Metric(Now, "s3c.ok", 1, "count"), Now), "missing id");

        var bad = WithId(IdleMonMessage.Metric(Now, "s3c.ok", 1, "count"));
        bad.Id = "has space";
        Assert.IsNotNull(IdleMonValidator.Validate(bad, Now), "id characters");

        var offset = WithId(IdleMonMessage.Metric(Now, "s3c.ok", 1, "count"));
        offset.Timestamp = "2026-11-05T12:00:00+00:00";
        Assert.IsNotNull(IdleMonValidator.Validate(offset, Now), "offset instead of Z");

        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now.AddMinutes(6), "s3c.ok", 1, "count")), Now), "more than 5 min ahead");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now.AddDays(-31), "s3c.ok", 1, "count")), Now), "older than 30 days");
        Assert.IsNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now.AddMinutes(4), "s3c.ok", 1, "count")), Now), "4 min ahead is fine");
    }

    [TestMethod]
    public void Validator_EnforcesTagLimits()
    {
        var tags = Enumerable.Range(0, 21).ToDictionary(i => $"t{i}", _ => "v");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "s3c.ok", 1, "count", tags)), Now), "21 tags");

        tags.Remove("t0");
        Assert.IsNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "s3c.ok", 1, "count", tags)), Now), "20 tags");

        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "s3c.ok", 1, "count", new() { ["bad key"] = "v" })), Now), "tag key characters");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "s3c.ok", 1, "count", new() { ["k"] = new string('x', 257) })), Now), "tag value length");
        Assert.IsNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "s3c.ok", 1, "count", new() { ["k"] = new string('x', 256) })), Now), "tag value 256");
    }

    [TestMethod]
    public void JobTags_AreCapped_AndFollowIncludeBucketNames()
    {
        var job = IdleMonTestData.Job(name: new string('n', 300));
        var withNames = IdleMonTags.ForJob(job, IdleMonTestData.Options());
        Assert.AreEqual(256, withNames["job"].Length);
        Assert.AreEqual("contoso-archive", withNames["bucketA"]);
        Assert.AreEqual("ListingOnly", withNames["mode"]);

        var anonymous = IdleMonTags.ForJob(job, IdleMonTestData.Options(o => o.IncludeBucketNames = false));
        Assert.AreEqual("A", anonymous["bucketA"]);
        Assert.AreEqual("B", anonymous["bucketB"]);
        Assert.IsNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Metric(Now, "s3c.ok", 1, "count", anonymous)), Now));
    }

    [TestMethod]
    public void Validator_ChecksAlertEventHeartbeatAndSnapshotRules()
    {
        var firingWithoutTitle = WithId(IdleMonMessage.AlertFiring(Now, "k", "error", "t", null));
        firingWithoutTitle.Title = " ";
        Assert.IsNotNull(IdleMonValidator.Validate(firingWithoutTitle, Now));

        Assert.IsNull(IdleMonValidator.Validate(WithId(IdleMonMessage.AlertResolved(Now, "k")), Now), "resolved needs no severity or title");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.AlertFiring(Now, "k", "fatal", "t", null)), Now), "severity set");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Event(Now, "info", "c", "   ")), Now), "whitespace message");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Event(Now, "notice", "c", "m")), Now), "event severity set");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Heartbeat(Now, 5, "ok", null)), Now), "interval below 10");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Heartbeat(Now, 60, "sleeping", null)), Now), "heartbeat status");

        var columns = new List<IdleMonColumn> { new() { Name = "n", Type = "number" }, new() { Name = "t", Type = "datetime" } };
        Assert.IsNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Snapshot(Now, "s3c.x", columns, [[1, "2026-11-05T12:00:00.000Z"], [null, null]])), Now));
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Snapshot(Now, "s3c.x", columns, [["text", null]])), Now), "number cell holds text");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Snapshot(Now, "s3c.x", columns, [[1, "2026-11-05T12:00:00+01:00"]])), Now), "datetime needs Z");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Snapshot(Now, "s3c.x", columns, [[1]])), Now), "row width");
        Assert.IsNotNull(IdleMonValidator.Validate(WithId(IdleMonMessage.Snapshot(Now, "s3c.x", [new() { Name = "a" }, new() { Name = "a" }], [])), Now), "duplicate column");
    }

    [TestMethod]
    public void Limits_CapKeyAndTitle()
    {
        var key = new string('k', 250);
        var capped = IdleMonLimits.CapKey(key);
        Assert.IsTrue(capped.Length <= 200);
        Assert.AreEqual(capped, IdleMonLimits.CapKey(key), "stable");
        Assert.AreEqual(200, IdleMonLimits.CapTitle(new string('t', 500)).Length);
        Assert.AreEqual("short", IdleMonLimits.CapTitle("short"));
    }

    [TestMethod]
    public void Reporter_AssignsIdOnce_DropsInvalidMessages_AndNeverThrows()
    {
        var clock = new FakeClock();
        var queue = new IdleMonQueue();
        var status = new IdleMonStatus { Queue = queue };
        var reporter = new IdleMonReporter(queue, status, clock, NullLogger<IdleMonReporter>.Instance);

        var good = IdleMonMessage.Metric(clock.GetUtcNow(), "s3c.ok", 1, "count");
        reporter.Submit(good);
        Assert.IsFalse(string.IsNullOrEmpty(good.Id));
        var id = good.Id;
        Assert.AreEqual(1, queue.Count);

        reporter.Submit(IdleMonMessage.Metric(clock.GetUtcNow(), "Bad Name", 1, "count"));
        Assert.AreEqual(1, queue.Count);
        Assert.AreEqual(1, status.Dropped);

        reporter.Submit(null!); // must not throw
        Assert.AreEqual(id, good.Id);
    }

    [TestMethod]
    public void Reporter_DropsMessagesOverTheSizeLimit()
    {
        var clock = new FakeClock();
        var queue = new IdleMonQueue();
        var status = new IdleMonStatus { Queue = queue };
        var reporter = new IdleMonReporter(queue, status, clock, NullLogger<IdleMonReporter>.Instance);
        var rows = Enumerable.Range(0, 400).Select(i => new object?[] { new string('x', 200) }).ToList();
        reporter.Submit(IdleMonMessage.Snapshot(clock.GetUtcNow(), "s3c.big", [new IdleMonColumn { Name = "k" }], rows));
        Assert.AreEqual(0, queue.Count);
        Assert.AreEqual(1, status.Dropped);
    }

    [TestMethod]
    public void Disabled_RegistersOnlyNoOps_NoSenderNoFilesNoHttp()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"idlemon_off_{Guid.NewGuid():N}");
        try
        {
            var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["IdleMon:Enabled"] = "false",
                ["IdleMon:SpoolDirectory"] = dir,
                ["IdleMon:DryRunFile"] = Path.Combine(dir, "dry.jsonl"),
            }).Build();
            var services = new ServiceCollection().AddLogging().AddIdleMon(config);
            using var provider = services.BuildServiceProvider();

            Assert.IsInstanceOfType<NullIdleMonReporter>(provider.GetRequiredService<IIdleMonReporter>());
            Assert.IsInstanceOfType<NoOpIdleMonEvents>(provider.GetRequiredService<IIdleMonEvents>());
            Assert.AreEqual(0, provider.GetServices<IHostedService>().Count(), "no hosted service at all");
            Assert.IsFalse(provider.GetRequiredService<IdleMonStatus>().Enabled);

            provider.GetRequiredService<IIdleMonReporter>().Submit(IdleMonMessage.Metric(DateTimeOffset.UtcNow, "s3c.ok", 1, "count"));
            Assert.IsFalse(Directory.Exists(dir), "disabled writes no files");
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    [TestMethod]
    public void Enabled_WithNonLoopbackAgent_IsSwitchedOffWithAProblemMessage()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["IdleMon:Enabled"] = "true",
            ["IdleMon:AgentUrl"] = "http://10.1.2.3:47100",
        }).Build();
        using var provider = new ServiceCollection().AddLogging().AddIdleMon(config).BuildServiceProvider();
        var status = provider.GetRequiredService<IdleMonStatus>();
        Assert.IsFalse(status.Enabled);
        StringAssert.Contains(status.ConfigProblem, "loopback");
        Assert.IsInstanceOfType<NullIdleMonReporter>(provider.GetRequiredService<IIdleMonReporter>());
    }

    [TestMethod]
    public void Enabled_RegistersSenderBeforeReportingService_SoTheSenderStopsLast()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["IdleMon:Enabled"] = "true",
            ["IdleMon:DryRunFile"] = Path.Combine(Path.GetTempPath(), $"idlemon_{Guid.NewGuid():N}.jsonl"),
        }).Build();
        var services = new ServiceCollection().AddLogging().AddIdleMon(config);
        var hosted = services.Where(d => d.ServiceType == typeof(IHostedService)).ToList();
        Assert.AreEqual(2, hosted.Count);
        Assert.IsNotNull(hosted[0].ImplementationFactory, "sender (factory) first");
        Assert.AreEqual(typeof(IdleMonReportingService), hosted[1].ImplementationType, "reporting service second");
    }

    [TestMethod]
    public void Options_Validate_RejectsBadValues()
    {
        Assert.IsNull(new IdleMonOptions().Validate());
        Assert.IsNotNull(new IdleMonOptions { AgentUrl = "http://example.com:47100" }.Validate());
        Assert.IsNotNull(new IdleMonOptions { AgentUrl = "not a url" }.Validate());
        Assert.IsNull(new IdleMonOptions { AgentUrl = "http://localhost:47100" }.Validate());
        Assert.IsNull(new IdleMonOptions { AgentUrl = "http://[::1]:47100" }.Validate());
        Assert.IsNotNull(new IdleMonOptions { Instance = "bad instance!" }.Validate());
        Assert.IsNotNull(new IdleMonOptions { HeartbeatIntervalSeconds = 5 }.Validate());
    }
}
