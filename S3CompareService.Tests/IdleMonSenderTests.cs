using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Worker.IdleMon;

namespace S3CompareService.Tests;

[TestClass]
public class IdleMonSenderTests
{
    private string _dir = "";

    [TestInitialize]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"idlemon_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    [TestCleanup]
    public void Cleanup()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    private sealed record Rig(IdleMonSender Sender, IdleMonReporter Reporter, IdleMonQueue Queue, IdleMonSpool Spool, IdleMonStatus Status, FakeHandler Handler, FakeClock Clock);

    private Rig NewRig(Func<HttpRequestMessage, HttpStatusCode> respond, Action<IdleMonOptions>? configure = null, long spoolBytes = 50L * 1024 * 1024)
    {
        var clock = new FakeClock();
        var options = IdleMonTestData.Options(configure);
        var queue = new IdleMonQueue();
        var spool = new IdleMonSpool(Path.Combine(_dir, "spool"), spoolBytes, clock);
        var status = new IdleMonStatus { Queue = queue, Spool = spool };
        var handler = new FakeHandler(respond);
        var sender = new IdleMonSender(options, queue, spool, status, clock, NullLogger<IdleMonSender>.Instance, "FS01", "1.0.0", handler);
        var reporter = new IdleMonReporter(queue, status, clock, NullLogger<IdleMonReporter>.Instance);
        return new Rig(sender, reporter, queue, spool, status, handler, clock);
    }

    private static List<(string Id, string Timestamp, string Kind)> MessagesOf(string body)
    {
        using var doc = JsonDocument.Parse(body);
        return doc.RootElement.GetProperty("messages").EnumerateArray()
            .Select(m => (m.GetProperty("id").GetString()!, m.GetProperty("timestamp").GetString()!, m.GetProperty("kind").GetString()!))
            .ToList();
    }

    private static void Metric(Rig rig, string name, double value) =>
        rig.Reporter.Submit(IdleMonMessage.Metric(rig.Clock.GetUtcNow(), name, value, "count"));

    [TestMethod]
    public async Task Envelope_HasSchemaVersionSourceAndMessages_AndApiKeyComesFromTheNamedEnvironmentVariable()
    {
        var envName = $"IDLEMON_TEST_KEY_{Guid.NewGuid():N}";
        Environment.SetEnvironmentVariable(envName, "unit-test-key");
        try
        {
            var rig = NewRig(_ => HttpStatusCode.Accepted, o => { o.ApiKeyEnv = envName; o.Instance = "prod"; });
            Metric(rig, "s3c.a", 1);
            await rig.Sender.FlushOnceAsync(false, CancellationToken.None);

            var request = rig.Handler.Requests.Single();
            Assert.AreEqual(HttpMethod.Post, request.Method);
            Assert.AreEqual("http://127.0.0.1:47100/api/v1/ingest", request.Uri!.ToString());
            Assert.AreEqual("unit-test-key", request.Headers["X-IdleMon-ApiKey"]);
            using var doc = JsonDocument.Parse(request.Body);
            Assert.AreEqual(1, doc.RootElement.GetProperty("schemaVersion").GetInt32());
            var source = doc.RootElement.GetProperty("source");
            Assert.AreEqual("S3CompareService", source.GetProperty("app").GetString());
            Assert.AreEqual("prod", source.GetProperty("instance").GetString());
            Assert.AreEqual("FS01", source.GetProperty("host").GetString());
            Assert.AreEqual(1, rig.Status.Accepted);
            Assert.IsNull(rig.Status.LastError);
        }
        finally
        {
            Environment.SetEnvironmentVariable(envName, null);
        }
    }

    [TestMethod]
    public async Task NoApiKeyHeader_WhenTheEnvironmentVariableIsNotSet()
    {
        var rig = NewRig(_ => HttpStatusCode.Accepted, o => o.ApiKeyEnv = $"IDLEMON_TEST_UNSET_{Guid.NewGuid():N}");
        Metric(rig, "s3c.a", 1);
        await rig.Sender.FlushOnceAsync(false, CancellationToken.None);
        Assert.IsFalse(rig.Handler.Requests.Single().Headers.ContainsKey("X-IdleMon-ApiKey"));
    }

    [TestMethod]
    public async Task Spool_ReplaysOldestFirst_KeepingOriginalIdsAndTimestamps_ThenLiveMessages()
    {
        var up = false;
        var rig = NewRig(_ => up ? HttpStatusCode.Accepted : HttpStatusCode.ServiceUnavailable);

        Metric(rig, "s3c.first", 1);
        var firstTimestamp = rig.Clock.GetUtcNow();
        await rig.Sender.FlushOnceAsync(true, CancellationToken.None); // agent down: spooled
        Assert.AreEqual(1, rig.Spool.Stats().Files);

        rig.Clock.Advance(TimeSpan.FromMinutes(5));
        Metric(rig, "s3c.second", 2);
        await rig.Sender.FlushOnceAsync(true, CancellationToken.None);
        Assert.AreEqual(2, rig.Spool.Stats().Files);

        rig.Clock.Advance(TimeSpan.FromMinutes(5));
        Metric(rig, "s3c.live", 3);
        rig.Handler.Requests.Clear();
        up = true;
        await rig.Sender.FlushOnceAsync(true, CancellationToken.None);

        var sent = rig.Handler.Requests.Select(r => MessagesOf(r.Body).Single()).ToList();
        Assert.AreEqual(3, sent.Count);
        Assert.AreEqual(IdleMonWire.Timestamp(firstTimestamp), sent[0].Timestamp, "original timestamp kept");
        Assert.AreEqual(IdleMonWire.Timestamp(firstTimestamp.AddMinutes(5)), sent[1].Timestamp);
        Assert.AreEqual(IdleMonWire.Timestamp(firstTimestamp.AddMinutes(10)), sent[2].Timestamp);
        Assert.AreEqual(3, sent.Select(s => s.Id).Distinct().Count());
        Assert.AreEqual(0, rig.Spool.Stats().Files, "spool is empty after the replay");
        Assert.AreEqual(0, rig.Queue.Count);
    }

    [TestMethod]
    public async Task SpooledIds_AreTheIdsOfTheOriginalMessages()
    {
        var up = false;
        var rig = NewRig(_ => up ? HttpStatusCode.Accepted : HttpStatusCode.ServiceUnavailable);
        var message = IdleMonMessage.Metric(rig.Clock.GetUtcNow(), "s3c.keep", 1, "count");
        rig.Reporter.Submit(message);
        var originalId = message.Id;
        await rig.Sender.FlushOnceAsync(true, CancellationToken.None);
        up = true;
        rig.Handler.Requests.Clear();
        await rig.Sender.FlushOnceAsync(true, CancellationToken.None);
        Assert.AreEqual(originalId, MessagesOf(rig.Handler.Requests.Single().Body).Single().Id);
    }

    [TestMethod]
    public async Task Backoff_DelaysTheNextAttempt_AndGrowsUpToSixtySeconds()
    {
        var rig = NewRig(_ => HttpStatusCode.ServiceUnavailable);
        Metric(rig, "s3c.a", 1);
        await rig.Sender.FlushOnceAsync(false, CancellationToken.None);
        var firstDelay = rig.Sender.NextAttempt - rig.Clock.GetUtcNow();
        Assert.IsTrue(firstDelay >= TimeSpan.FromMilliseconds(700) && firstDelay <= TimeSpan.FromMilliseconds(1300), $"about 1 s, was {firstDelay}");

        // During the backoff nothing is attempted: the live messages go straight to the spool.
        var requests = rig.Handler.Requests.Count;
        Metric(rig, "s3c.b", 2);
        await rig.Sender.FlushOnceAsync(false, CancellationToken.None);
        Assert.AreEqual(requests, rig.Handler.Requests.Count);
        Assert.AreEqual(0, rig.Queue.Count);

        for (var i = 0; i < 12; i++)
        {
            rig.Clock.Advance(TimeSpan.FromMinutes(2));
            await rig.Sender.FlushOnceAsync(false, CancellationToken.None);
        }

        var delay = rig.Sender.NextAttempt - rig.Clock.GetUtcNow();
        Assert.IsTrue(delay <= TimeSpan.FromSeconds(72) && delay >= TimeSpan.FromSeconds(48), $"capped near 60 s, was {delay}");
        StringAssert.Contains(rig.Status.LastError, "503");
    }

    [TestMethod]
    public async Task Unauthorized_KeepsMessagesInTheSpool()
    {
        var rig = NewRig(_ => HttpStatusCode.Unauthorized);
        Metric(rig, "s3c.a", 1);
        await rig.Sender.FlushOnceAsync(true, CancellationToken.None);
        Assert.AreEqual(1, rig.Spool.Stats().Files);
        StringAssert.Contains(rig.Status.LastError, "401");
    }

    [TestMethod]
    public async Task BadRequest_DropsTheBatch_AndDoesNotRetryIt()
    {
        var rig = NewRig(_ => HttpStatusCode.BadRequest);
        Metric(rig, "s3c.a", 1);
        await rig.Sender.FlushOnceAsync(true, CancellationToken.None);
        Assert.AreEqual(0, rig.Spool.Stats().Files);
        Assert.AreEqual(1, rig.Status.Dropped);
    }

    [TestMethod]
    public async Task DryRun_WritesJsonLinesToTheFile_AndSendsNothing()
    {
        var file = Path.Combine(_dir, "out", "dry.jsonl");
        var clock = new FakeClock();
        var options = IdleMonTestData.Options(o => o.DryRunFile = file);
        var queue = new IdleMonQueue();
        var status = new IdleMonStatus { Queue = queue };
        var sender = new IdleMonSender(options, queue, null, status, clock, NullLogger<IdleMonSender>.Instance, "FS01", "1.0.0", new FakeHandler(_ => throw new AssertFailedException("no HTTP in dry-run mode")));
        var reporter = new IdleMonReporter(queue, status, clock, NullLogger<IdleMonReporter>.Instance);
        reporter.Submit(IdleMonMessage.Metric(clock.GetUtcNow(), "s3c.a", 1, "count"));
        await sender.FlushOnceAsync(false, CancellationToken.None);

        var lines = File.ReadAllLines(file);
        Assert.AreEqual(1, lines.Length);
        Assert.AreEqual(1, MessagesOf(lines[0]).Count);
        Assert.AreEqual(1, status.Accepted);
    }

    [TestMethod]
    public void Spool_OverItsCap_DropsInfoEventsFirst_AndNeverAlerts()
    {
        var clock = new FakeClock();
        var now = clock.GetUtcNow();
        byte[] Envelope(params IdleMonMessage[] messages)
        {
            foreach (var m in messages) m.Id = Guid.NewGuid().ToString("N");
            return IdleMonWire.BuildEnvelope("FS01", "1.0.0", "default", now, messages.Select(IdleMonWire.Serialize).ToList());
        }

        var big = new string('x', 800);
        var spool = new IdleMonSpool(Path.Combine(_dir, "capped"), 6000, clock);
        spool.Write([Envelope(
            IdleMonMessage.Event(now, "info", "scan", big),
            IdleMonMessage.AlertFiring(now, "run-failed/j1", "error", "failed", "why"),
            IdleMonMessage.Event(now, "debug", "scan", big))]);
        spool.Write([Envelope(IdleMonMessage.Event(now, "error", "scan", "failed: " + big), IdleMonMessage.Metric(now, "s3c.a", 1, "count"))]);
        spool.Write([Envelope(IdleMonMessage.Event(now, "info", "scan", big), IdleMonMessage.Event(now, "info", "scan", big), IdleMonMessage.Event(now, "info", "scan", big))]);
        Assert.IsTrue(spool.Stats().Bytes <= 6000, $"spool is {spool.Stats().Bytes} bytes");

        var all = new List<string>();
        while (spool.PeekOldest() is { } oldest)
        {
            foreach (var envelope in oldest.Envelopes) all.Add(System.Text.Encoding.UTF8.GetString(envelope));
            spool.Delete(oldest.Path);
        }

        var text = string.Join("\n", all);
        StringAssert.Contains(text, "run-failed/j1", "the alert survives");
        StringAssert.Contains(text, "\"severity\":\"error\"", "error events survive");
        Assert.IsTrue(spool.DroppedMessages > 0, "info events were dropped");
        Assert.IsFalse(text.Contains("\"severity\":\"debug\""), "oldest debug event went first");
    }

    [TestMethod]
    public void Spool_WhenOnlyAlertsRemain_KeepsThemEvenOverTheCap()
    {
        var clock = new FakeClock();
        var now = clock.GetUtcNow();
        var spool = new IdleMonSpool(Path.Combine(_dir, "alerts"), 300, clock);
        for (var i = 0; i < 5; i++)
        {
            var alert = IdleMonMessage.AlertFiring(now, $"run-failed/j{i}", "error", "failed", new string('m', 300));
            alert.Id = Guid.NewGuid().ToString("N");
            spool.Write([IdleMonWire.BuildEnvelope("FS01", "1.0.0", "default", now, [IdleMonWire.Serialize(alert)])]);
        }

        Assert.AreEqual(5, spool.Stats().Files);
        Assert.AreEqual(0, spool.DroppedMessages);
    }

    [TestMethod]
    public void Spool_OldestMessageTime_IsTheEarliestTimestampInTheOldestFile()
    {
        var clock = new FakeClock();
        var spool = new IdleMonSpool(Path.Combine(_dir, "age"), 1_000_000, clock);
        Assert.IsNull(spool.OldestMessageTime());
        var early = clock.GetUtcNow().AddMinutes(-90);
        var m1 = IdleMonMessage.Metric(early, "s3c.a", 1, "count"); m1.Id = "a1";
        var m2 = IdleMonMessage.Metric(early.AddMinutes(30), "s3c.a", 2, "count"); m2.Id = "a2";
        spool.Write([IdleMonWire.BuildEnvelope("FS01", "1.0.0", "default", clock.GetUtcNow(), [IdleMonWire.Serialize(m2), IdleMonWire.Serialize(m1)])]);
        Assert.AreEqual(early.ToUnixTimeSeconds(), spool.OldestMessageTime()!.Value.ToUnixTimeSeconds());
    }

    [TestMethod]
    public async Task LoopbackOnlyHandler_RefusesEveryOtherHostMethodAndPath_WithoutTouchingTheNetwork()
    {
        var inner = new FakeHandler(_ => HttpStatusCode.Accepted);
        using var client = new HttpClient(new LoopbackOnlyHandler(inner, new Uri("http://127.0.0.1:47100/")));

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => client.PostAsync("http://example.com/api/v1/ingest", new StringContent("{}")));
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => client.PostAsync("http://10.0.0.8:47100/api/v1/ingest", new StringContent("{}")));
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => client.GetAsync("http://127.0.0.1:47100/api/v1/ingest"));
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => client.PostAsync("http://127.0.0.1:47100/api/v1/other", new StringContent("{}")));
        Assert.AreEqual(0, inner.Requests.Count);

        using var ok = await client.PostAsync("http://127.0.0.1:47100/api/v1/ingest", new StringContent("{}"));
        Assert.AreEqual(1, inner.Requests.Count);
        using var okLocalhost = await client.PostAsync("http://localhost:47100/api/v1/ingest", new StringContent("{}"));
        using var okV6 = await client.PostAsync("http://[::1]:47100/api/v1/ingest", new StringContent("{}"));
        Assert.AreEqual(3, inner.Requests.Count);
    }

    [TestMethod]
    public async Task Sender_TreatsAGuardRefusalAsRetryable_AndNothingLeavesTheMachine()
    {
        // Even a hand-built options object that points elsewhere cannot send: the guard refuses and the batch is spooled.
        var inner = new FakeHandler(_ => HttpStatusCode.Accepted);
        var clock = new FakeClock();
        var options = IdleMonTestData.Options(o => o.AgentUrl = "http://192.0.2.10:47100");
        var queue = new IdleMonQueue();
        var spool = new IdleMonSpool(Path.Combine(_dir, "refused"), 1_000_000, clock);
        var status = new IdleMonStatus { Queue = queue, Spool = spool };
        var sender = new IdleMonSender(options, queue, spool, status, clock, NullLogger<IdleMonSender>.Instance, "FS01", "1.0.0", inner);
        new IdleMonReporter(queue, status, clock, NullLogger<IdleMonReporter>.Instance).Submit(IdleMonMessage.Metric(clock.GetUtcNow(), "s3c.a", 1, "count"));
        await sender.FlushOnceAsync(true, CancellationToken.None);
        Assert.AreEqual(0, inner.Requests.Count);
        Assert.AreEqual(1, spool.Stats().Files);
    }

    [TestMethod]
    public void Queue_WhenFull_DropsLowPriorityFirst_AndNeverAlerts()
    {
        var queue = new IdleMonQueue(capacity: 3);
        byte[] B(string s) => System.Text.Encoding.UTF8.GetBytes(s);
        queue.Add(new PendingMessage(B("info1"), MessagePriority.Low));
        queue.Add(new PendingMessage(B("metric1"), MessagePriority.Normal));
        queue.Add(new PendingMessage(B("alert1"), MessagePriority.High));
        queue.Add(new PendingMessage(B("alert2"), MessagePriority.High)); // evicts info1
        queue.Add(new PendingMessage(B("alert3"), MessagePriority.High)); // evicts metric1
        queue.Add(new PendingMessage(B("alert4"), MessagePriority.High)); // nothing left to evict: grows
        Assert.IsFalse(queue.Add(new PendingMessage(B("info2"), MessagePriority.Low)), "a new info event cannot displace an alert");
        var taken = queue.Take(100, 100000).Select(m => System.Text.Encoding.UTF8.GetString(m.Json)).ToList();
        CollectionAssert.AreEqual(new[] { "alert1", "alert2", "alert3", "alert4" }, taken);
    }
}
