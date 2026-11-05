using S3CompareService.Core.IdleMon;

namespace S3CompareService.Tests;

[TestClass]
public class IdleMonCardTests
{
    private static readonly DateTime Now = new(2026, 1, 15, 12, 0, 0, DateTimeKind.Utc);

    private static IdleMonStatusDto Http(Action<IdleMonStatusDto>? edit = null)
    {
        var s = new IdleMonStatusDto { Enabled = true, Mode = "Http", HeartbeatIntervalSeconds = 60, LastSendAt = Now.AddSeconds(-20) };
        edit?.Invoke(s);
        return s;
    }

    [TestMethod]
    public void ServiceOffline_IsUnknown() => Assert.AreEqual(IdleMonCardState.Unknown, IdleMonCard.Derive(null, Now));

    [TestMethod]
    public void Disabled_IsOff() =>
        Assert.AreEqual(IdleMonCardState.Off, IdleMonCard.Derive(new IdleMonStatusDto { Enabled = false, Mode = "Disabled" }, Now));

    [TestMethod]
    public void ConfigProblem_WinsEvenThoughReportingIsOff()
    {
        var s = new IdleMonStatusDto { Enabled = false, Mode = "Disabled", ConfigProblem = "IdleMon:AgentUrl must be a loopback address" };
        Assert.AreEqual(IdleMonCardState.ConfigProblem, IdleMonCard.Derive(s, Now));
    }

    [TestMethod]
    public void DryRun_IsDryRun() =>
        Assert.AreEqual(IdleMonCardState.DryRun, IdleMonCard.Derive(Http(s => s.Mode = "DryRun"), Now));

    [TestMethod]
    public void RecentSendWithoutError_IsSending() =>
        Assert.AreEqual(IdleMonCardState.Sending, IdleMonCard.Derive(Http(), Now));

    [TestMethod]
    public void SendJustInsideTwoHeartbeats_IsSending_JustOutsideIsNot()
    {
        Assert.AreEqual(IdleMonCardState.Sending, IdleMonCard.Derive(Http(s => s.LastSendAt = Now.AddSeconds(-120)), Now));
        Assert.AreEqual(IdleMonCardState.Waiting, IdleMonCard.Derive(Http(s => s.LastSendAt = Now.AddSeconds(-121)), Now));
    }

    [TestMethod]
    public void LastError_IsSpooling_EvenWithARecentSend() =>
        Assert.AreEqual(IdleMonCardState.Spooling, IdleMonCard.Derive(Http(s => s.LastError = "connection refused"), Now));

    [TestMethod]
    public void SpoolFiles_AreSpooling_EvenWithoutAnError() =>
        Assert.AreEqual(IdleMonCardState.Spooling, IdleMonCard.Derive(Http(s => s.SpoolFiles = 2), Now));

    [TestMethod]
    public void EnabledButNeverSent_IsWaiting() =>
        Assert.AreEqual(IdleMonCardState.Waiting, IdleMonCard.Derive(Http(s => s.LastSendAt = null), Now));

    [TestMethod]
    public void UnspecifiedKind_TimestampsAreTreatedAsUtc()
    {
        var s = Http(x => x.LastSendAt = new DateTime(2026, 1, 15, 11, 59, 30, DateTimeKind.Unspecified));
        Assert.AreEqual(IdleMonCardState.Sending, IdleMonCard.Derive(s, Now));
    }

    [TestMethod]
    public void Relative_And_Size_Formatting()
    {
        Assert.AreEqual("never", IdleMonCard.Relative(null, Now));
        Assert.AreEqual("5 s ago", IdleMonCard.Relative(Now.AddSeconds(-5), Now));
        Assert.AreEqual("3 min ago", IdleMonCard.Relative(Now.AddMinutes(-3), Now));
        Assert.AreEqual("5 h ago", IdleMonCard.Relative(Now.AddHours(-5), Now));
        Assert.AreEqual("0 s ago", IdleMonCard.Relative(Now.AddSeconds(30), Now), "a clock that runs ahead never shows a negative age");
        Assert.AreEqual("512 B", IdleMonCard.Size(512));
        Assert.AreEqual("12 KB", IdleMonCard.Size(12 * 1024));
        Assert.AreEqual("1.5 MB", IdleMonCard.Size(1536 * 1024));
        Assert.AreEqual("Agent unreachable — spooling", IdleMonCard.Label(IdleMonCardState.Spooling));
    }
}
