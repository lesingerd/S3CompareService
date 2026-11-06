using System.Text;
using System.Text.Json;
using S3CompareService.Worker.IdleMon;

namespace S3CompareService.Tests;

/// <summary>Spool behaviour over weeks of outage: running totals, alert coalescing, the hard ceiling, unreadable files.</summary>
[TestClass]
public class IdleMonSpoolHardeningTests
{
    private string _dir = null!;
    private FakeClock _clock = null!;
    private readonly List<string> _warnings = [];

    [TestInitialize]
    public void Init()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"idlemon_spoolhard_{Guid.NewGuid():N}");
        _clock = new FakeClock();
    }

    [TestCleanup]
    public void Cleanup()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    private IdleMonSpool NewSpool(long maxBytes = 50_000_000) => new(_dir, maxBytes, _clock, _warnings.Add);

    private byte[] Envelope(params IdleMonMessage[] messages)
    {
        foreach (var m in messages) m.Id = Guid.NewGuid().ToString("N");
        return IdleMonWire.BuildEnvelope("FS01", "1.0.0", "default", _clock.GetUtcNow(), messages.Select(IdleMonWire.Serialize).ToList());
    }

    private IdleMonMessage Firing(string key, string text = "x") => IdleMonMessage.AlertFiring(_clock.GetUtcNow(), key, "error", "failed", text);

    private IdleMonMessage Resolved(string key) => IdleMonMessage.AlertResolved(_clock.GetUtcNow(), key);

    /// <summary>Everything in the spool, oldest first, as (kind, key, status, message) tuples.</summary>
    private static List<(string? Kind, string? Key, string? Status, string? Message)> Contents(IdleMonSpool spool)
    {
        var all = new List<(string?, string?, string?, string?)>();
        foreach (var file in Directory.GetFiles(spool.Directory, "spool-*.jsonl").Order(StringComparer.Ordinal))
        {
            foreach (var line in File.ReadAllLines(file).Where(l => l.Length > 0))
            {
                using var doc = JsonDocument.Parse(line);
                foreach (var m in doc.RootElement.GetProperty("messages").EnumerateArray())
                {
                    string? Get(string n) => m.TryGetProperty(n, out var v) ? v.GetString() : null;
                    all.Add((Get("kind"), Get("key"), Get("status"), Get("message")));
                }
            }
        }

        return all;
    }

    private static long SizeOnDisk(IdleMonSpool spool) => Directory.GetFiles(spool.Directory, "spool-*.jsonl").Sum(f => new FileInfo(f).Length);

    [TestMethod]
    public void RunningTotals_MatchTheDisk_AfterWriteRewriteAndDelete()
    {
        var spool = NewSpool();
        var p1 = spool.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.a", 1, "count"))]);
        var p2 = spool.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.b", 2, "count"), IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.c", 3, "count"))]);
        Assert.AreEqual((2, SizeOnDisk(spool)), spool.Stats());

        spool.Rewrite(p2, [Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.c", 3, "count"))]);
        Assert.AreEqual((2, SizeOnDisk(spool)), spool.Stats());

        spool.Delete(p1);
        Assert.AreEqual((1, SizeOnDisk(spool)), spool.Stats());
        spool.Delete(p2);
        Assert.AreEqual((0, 0L), spool.Stats());
        Assert.IsNull(spool.OldestMessageTime());
    }

    [TestMethod]
    public void StatsAndOldestMessageTime_ComeFromMemory_NotFromTheDisk()
    {
        var spool = NewSpool();
        var old = IdleMonMessage.Metric(_clock.GetUtcNow().AddMinutes(-30), "s3c.old", 1, "count");
        spool.Write([Envelope(old)]);
        var stats = spool.Stats();
        var oldest = spool.OldestMessageTime();

        Directory.Delete(_dir, true); // a heartbeat or status call must not need the disk

        Assert.AreEqual(stats, spool.Stats());
        Assert.AreEqual(oldest, spool.OldestMessageTime());
        Assert.AreEqual(_clock.GetUtcNow().AddMinutes(-30).ToUnixTimeSeconds(), oldest!.Value.ToUnixTimeSeconds());
    }

    [TestMethod]
    public void AFreshSpoolOverAnExistingFolder_IndexesTheFiles_AndKeepsEnforcingTheCap()
    {
        var first = NewSpool();
        first.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.a", 1, "count"))]);
        first.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.b", 1, "count"))]);

        var second = NewSpool(maxBytes: 600);
        Assert.AreEqual(first.Stats(), second.Stats());
        Assert.IsNotNull(second.OldestMessageTime(), "taken from the file name");

        // Info events fill it past the cap: the metrics left by the earlier run are dropped (tier 2), the newest write survives.
        var big = new string('x', 700);
        second.Write([Envelope(IdleMonMessage.Event(_clock.GetUtcNow(), "info", "scan", big))]);
        Assert.IsTrue(second.Stats().Bytes <= 2000);
        Assert.AreEqual((second.Stats().Files, SizeOnDisk(second)), second.Stats());
    }

    [TestMethod]
    public void RepeatedAlertRefreshes_AreCoalesced_ToTheLatestPerKey_WhenOverTheCap()
    {
        var spool = NewSpool(maxBytes: 6000);
        for (var i = 0; i < 40; i++)
        {
            spool.Write([Envelope(Firing("disparities/j1", $"refresh {i}"), Firing("disparities/j2", $"refresh {i}"))]);
            _clock.Advance(TimeSpan.FromMinutes(5));
        }

        spool.Write([Envelope(Resolved("disparities/j1"))]); // the latest state of j1 is "resolved"

        var alerts = Contents(spool).Where(c => c.Kind == "alert").ToList();
        Assert.AreEqual(1, alerts.Count(a => a.Key == "disparities/j1"), "one message per key");
        Assert.AreEqual(1, alerts.Count(a => a.Key == "disparities/j2"));
        Assert.AreEqual("resolved", alerts.Single(a => a.Key == "disparities/j1").Status, "the latest state wins, not the first");
        Assert.AreEqual("refresh 39", alerts.Single(a => a.Key == "disparities/j2").Message);
        Assert.AreEqual((spool.Stats().Files, SizeOnDisk(spool)), spool.Stats());
        Assert.AreEqual(0, spool.DroppedMessages, "replaced refreshes are not 'lost' messages");
    }

    [TestMethod]
    public void RepeatedAlertRefreshes_AreCoalesced_OnEveryWrite_EvenFarUnderTheCap()
    {
        var spool = NewSpool(); // 50 MB: never over the cap
        for (var i = 0; i < IdleMonSpool.CoalesceEveryAlerts + 10; i++)
        {
            spool.Write([Envelope(Firing("run-failed/j1", $"n{i}"), IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.m", i, "count"))]);
        }

        var contents = Contents(spool);
        var alerts = contents.Where(c => c.Kind == "alert").ToList();
        Assert.AreEqual(1, alerts.Count, "one alert message however many refreshes were spooled");
        Assert.AreEqual($"n{IdleMonSpool.CoalesceEveryAlerts + 9}", alerts.Single().Message, "the newest refresh is the one that stays");
        Assert.AreEqual(IdleMonSpool.CoalesceEveryAlerts + 10, contents.Count(c => c.Kind == "metric"), "other messages are untouched");
        Assert.AreEqual((spool.Stats().Files, SizeOnDisk(spool)), spool.Stats());
    }

    [TestMethod]
    public void AlertsInFilesLeftByAnEarlierRun_AreCoalescedWhenTheSpoolGoesOverItsCap()
    {
        // A new spool object per write: the files are "left by an earlier run", unknown to the next one.
        for (var i = 0; i < 6; i++) NewSpool().Write([Envelope(Firing("run-failed/j1", $"old {i}"), IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.m", i, "count"))]);
        Assert.AreEqual(6, Contents(NewSpool()).Count(c => c.Kind == "alert"));

        var restarted = NewSpool(maxBytes: 2500);
        for (var i = 0; i < 6; i++) restarted.Write([Envelope(IdleMonMessage.Event(_clock.GetUtcNow(), "error", "scan", $"{i} {new string('z', 300)}"))]);

        var alerts = Contents(restarted).Where(c => c.Kind == "alert").ToList();
        Assert.AreEqual(1, alerts.Count);
        Assert.AreEqual("old 5", alerts.Single().Message);
    }

    [TestMethod]
    public void HardCeiling_DropsOldErrorEvents_ButNeverTheLatestAlertOfAKey()
    {
        var cap = 3000;
        var spool = NewSpool(cap);
        var big = new string('e', 500);
        spool.Write([Envelope(Firing("a/j1", "keep me"), Firing("a/j2", "keep me too"))]);
        for (var i = 0; i < 40; i++)
        {
            spool.Write([Envelope(IdleMonMessage.Event(_clock.GetUtcNow(), "error", "scan", $"{i} {big}"), Firing("a/j1", "keep me"))]);
        }

        Assert.IsTrue(spool.Stats().Bytes <= cap * 2 + 1500, $"spool is {spool.Stats().Bytes} bytes (ceiling {cap * 2})");
        var contents = Contents(spool);
        Assert.AreEqual(1, contents.Count(c => c.Key == "a/j1"));
        Assert.AreEqual(1, contents.Count(c => c.Key == "a/j2"), "j2 appears once and is never dropped");
        Assert.IsTrue(spool.DroppedMessages > 0, "the oldest error events went");
        Assert.IsTrue(contents.Any(c => c.Kind == "event" && c.Message!.StartsWith("39 ")), "the newest error event is kept");
    }

    [TestMethod]
    public void AnUnreadableFile_IsSkipped_SoNewerFilesAreStillSent_AndRetriedLater()
    {
        var spool = NewSpool();
        var stuck = spool.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.a", 1, "count"))]);
        var good = spool.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.b", 2, "count"))]);

        using (new FileStream(stuck, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            var peek = spool.PeekOldest();
            Assert.AreEqual(good, peek!.Value.Path, "the readable file is not blocked by the stuck one");
            Assert.AreEqual(1, _warnings.Count(w => w.Contains("Could not read spool file")), "warned once");

            // Not retried before 30 s have passed.
            spool.Delete(good);
            Assert.IsNull(spool.PeekOldest());
        }

        _clock.Advance(IdleMonSpool.UnreadableRetry + TimeSpan.FromSeconds(1));
        var again = spool.PeekOldest();
        Assert.AreEqual(stuck, again!.Value.Path, "retried once the lock is gone");
    }

    [TestMethod]
    public void AFileUnreadableFor24Hours_IsRenamedToBad_AndCountedAsDropped()
    {
        var spool = NewSpool();
        var stuck = spool.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.a", 1, "count"), IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.b", 1, "count"))]);
        var good = spool.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.c", 2, "count"))]);

        // Readers are refused, but renaming is allowed (FileShare.Delete).
        using (new FileStream(stuck, FileMode.Open, FileAccess.Read, FileShare.Delete))
        {
            Assert.AreEqual(good, spool.PeekOldest()!.Value.Path);
            for (var hour = 0; hour < 26; hour++)
            {
                _clock.Advance(TimeSpan.FromHours(1));
                spool.PeekOldest();
            }
        }

        Assert.IsFalse(File.Exists(stuck));
        Assert.IsTrue(File.Exists(stuck + ".bad"));
        Assert.AreEqual(1, spool.BadFiles);
        Assert.AreEqual(2, spool.DroppedMessages, "the two messages the file held");
        Assert.AreEqual(1, spool.Stats().Files);
    }

    [TestMethod]
    public void ADeleteThatFails_DoesNotMakeTheSenderResendTheSameFileInALoop()
    {
        var spool = NewSpool();
        var path = spool.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.a", 1, "count"))]);
        var peek = spool.PeekOldest();
        Assert.AreEqual(path, peek!.Value.Path);

        using (var handle = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)) // blocks the delete
        {
            spool.Delete(path); // must not throw
            Assert.IsNull(spool.PeekOldest(), "a file that was sent is not returned again");
            Assert.IsNotNull(handle);
        }

        _clock.Advance(TimeSpan.FromMinutes(1));
        Assert.IsNull(spool.PeekOldest());
        Assert.IsFalse(File.Exists(path), "the delete is retried later");
        Assert.AreEqual(0, spool.Stats().Files);
    }

    [TestMethod]
    public void WhenTheWallClockStepsBackwards_FilesStillReplayInWriteOrder()
    {
        var spool = NewSpool();
        var first = spool.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.a", 1, "count"))]);
        _clock.StepWallClock(TimeSpan.FromHours(-2));
        var second = spool.Write([Envelope(IdleMonMessage.Metric(_clock.GetUtcNow(), "s3c.b", 2, "count"))]);

        Assert.IsTrue(string.CompareOrdinal(Path.GetFileName(first), Path.GetFileName(second)) < 0);
        Assert.AreEqual(first, spool.PeekOldest()!.Value.Path);
    }

    [TestMethod]
    public void NonAlertMessages_AreDroppedOldestFirst_AndTheTotalsStayExact()
    {
        var spool = NewSpool(maxBytes: 3000);
        var big = new string('i', 700);
        for (var i = 0; i < 12; i++)
        {
            spool.Write([Envelope(IdleMonMessage.Event(_clock.GetUtcNow(), "info", "scan", $"{i} {big}"))]);
        }

        Assert.IsTrue(spool.Stats().Bytes <= 3000);
        Assert.AreEqual((spool.Stats().Files, SizeOnDisk(spool)), spool.Stats());
        var remaining = Contents(spool).Select(c => c.Message!).ToList();
        Assert.IsTrue(remaining.Last().StartsWith("11 "), "the newest event survives");
        Assert.IsFalse(remaining.Any(m => m.StartsWith("0 ")), "the oldest event went first");
        Assert.IsTrue(spool.DroppedMessages >= 8);
        StringAssert.Contains(string.Join("|", _warnings), "removed");
        Assert.IsNotNull(Encoding.UTF8);
    }
}
