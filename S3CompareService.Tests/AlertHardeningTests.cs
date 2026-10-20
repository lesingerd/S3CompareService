using System.Net;
using System.Net.Sockets;
using System.Text;
using S3CompareService.Core.Models;
using S3CompareService.Core.Notifications;

namespace S3CompareService.Tests;

[TestClass]
public class AlertHardeningTests
{
    /// <summary>Minimal loopback HTTP server answering every request with a fixed status code.</summary>
    private sealed class TinyHttpServer : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _cts = new();
        public int RequestCount;
        public string Url => $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";

        public TinyHttpServer(int statusCode)
        {
            _listener.Start();
            _ = Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    TcpClient client;
                    try { client = await _listener.AcceptTcpClientAsync(_cts.Token); }
                    catch { return; }

                    _ = Task.Run(async () =>
                    {
                        using (client)
                        {
                            var stream = client.GetStream();
                            var buf = new byte[8192];
                            var sb = new StringBuilder();
                            // read headers
                            while (!sb.ToString().Contains("\r\n\r\n"))
                            {
                                int n = await stream.ReadAsync(buf);
                                if (n == 0) return;
                                sb.Append(Encoding.ASCII.GetString(buf, 0, n));
                            }
                            // drain body if Content-Length present
                            string text = sb.ToString();
                            int headerEnd = text.IndexOf("\r\n\r\n", StringComparison.Ordinal) + 4;
                            int cl = 0;
                            foreach (var line in text[..headerEnd].Split("\r\n"))
                                if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                                    cl = int.Parse(line.Substring(15).Trim());
                            int have = text.Length - headerEnd;
                            while (have < cl)
                            {
                                int n = await stream.ReadAsync(buf);
                                if (n == 0) break;
                                have += n;
                            }
                            Interlocked.Increment(ref RequestCount);
                            string reason = statusCode == 200 ? "OK" : "Error";
                            var resp = Encoding.ASCII.GetBytes($"HTTP/1.1 {statusCode} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                            await stream.WriteAsync(resp);
                        }
                    });
                }
            });
        }

        public void Dispose() { _cts.Cancel(); _listener.Stop(); }
    }

    private static ComparisonJob JobWithWebhook(string url)
    {
        var job = new ComparisonJob { Name = "alert job" };
        job.AlertConfig.IsEnabled = true;
        job.AlertConfig.SendDailyDigest = true;
        job.AlertConfig.AzureWebhookUrl = url;
        return job;
    }

    private static ScanRun Completed(long filtered = 0, DateTime? end = null) => new()
    {
        JobId = "j", JobName = "n", Status = RunStatus.Completed,
        StartTimeUtc = (end ?? DateTime.UtcNow).AddMinutes(-5), EndTimeUtc = end ?? DateTime.UtcNow,
        FilteredDifferencesCount = filtered, TotalDifferences = filtered, ObjectsCompared = 100, MatchedCount = 100 - filtered
    };

    // ---------- 11: threshold semantics ----------

    [TestMethod]
    public async Task PerTypeThresholdsOfZero_AreDisabled_WhenOverallThresholdNotExceeded()
    {
        var job = new ComparisonJob { Name = "thr" };
        job.AlertConfig.IsEnabled = true;
        job.AlertConfig.MaxDisparitiesThreshold = 10;   // overall: tolerate up to 10
        // all per-type thresholds stay at the default 0 => disabled
        var run = Completed(filtered: 5);
        run.MissingInBucketBCount = 5;

        var svc = new AlertNotificationService();
        var (should, reasons) = await svc.EvaluateAlertConditionAsync(job, run);
        Assert.IsFalse(should, string.Join("|", reasons));
    }

    [TestMethod]
    public async Task AlertFires_WhenOverallExceeded_OrAnyEnabledPerTypeExceeded()
    {
        var svc = new AlertNotificationService();

        // overall exceeded
        var job1 = new ComparisonJob();
        job1.AlertConfig.IsEnabled = true;
        job1.AlertConfig.MaxDisparitiesThreshold = 3;
        var run1 = Completed(filtered: 4);
        run1.SizeMismatchCount = 4;
        var (should1, reasons1) = await svc.EvaluateAlertConditionAsync(job1, run1);
        Assert.IsTrue(should1);
        Assert.AreEqual(1, reasons1.Count, "disabled per-type thresholds add no reasons");

        // overall tolerated but an enabled per-type threshold is exceeded
        var job2 = new ComparisonJob();
        job2.AlertConfig.IsEnabled = true;
        job2.AlertConfig.MaxDisparitiesThreshold = 100;
        job2.AlertConfig.MaxMissingInBThreshold = 2;
        job2.AlertConfig.MaxSizeMismatchThreshold = 50;
        var run2 = Completed(filtered: 10);
        run2.MissingInBucketBCount = 3;
        run2.SizeMismatchCount = 7;
        var (should2, reasons2) = await svc.EvaluateAlertConditionAsync(job2, run2);
        Assert.IsTrue(should2);
        Assert.AreEqual(1, reasons2.Count);
        StringAssert.Contains(reasons2[0], "Missing in Bucket B");

        // negative per-type thresholds are also "disabled"
        var job3 = new ComparisonJob();
        job3.AlertConfig.IsEnabled = true;
        job3.AlertConfig.MaxDisparitiesThreshold = 100;
        job3.AlertConfig.MaxContentMismatchThreshold = -1;
        var run3 = Completed(filtered: 10);
        run3.ContentMismatchCount = 10;
        Assert.IsFalse((await svc.EvaluateAlertConditionAsync(job3, run3)).ShouldAlert);

        // default overall threshold of 0 still means "any confirmed disparity"
        var job4 = new ComparisonJob();
        job4.AlertConfig.IsEnabled = true;
        Assert.IsTrue((await svc.EvaluateAlertConditionAsync(job4, Completed(filtered: 1))).ShouldAlert);
        Assert.IsFalse((await svc.EvaluateAlertConditionAsync(job4, Completed(filtered: 0))).ShouldAlert);
    }

    // ---------- 12: digest assessment ----------

    [TestMethod]
    public void DigestAssessment_AllClearOnlyForRecentSuccessfulCleanRun()
    {
        var now = DateTime.UtcNow;
        var clean = Completed(0, now.AddHours(-2));
        Assert.IsTrue(AlertNotificationService.AssessDigest(clean, new List<ScanRun> { clean }, now).IsAllClear);

        Assert.IsFalse(AlertNotificationService.AssessDigest(null, new List<ScanRun>(), now).IsAllClear, "no run at all");

        var stale = Completed(0, now.AddHours(-30));
        var staleResult = AlertNotificationService.AssessDigest(stale, new List<ScanRun>(), now);
        Assert.IsFalse(staleResult.IsAllClear);
        StringAssert.Contains(staleResult.Issues[0], "hours ago");

        var failed = Completed(0, now.AddHours(-1));
        failed.Status = RunStatus.Failed;
        Assert.IsFalse(AlertNotificationService.AssessDigest(failed, new List<ScanRun> { failed }, now).IsAllClear);

        var disparities = Completed(7, now.AddHours(-1));
        var d = AlertNotificationService.AssessDigest(disparities, new List<ScanRun> { disparities }, now);
        Assert.IsFalse(d.IsAllClear);
        StringAssert.Contains(d.Issues[0], "7");

        // a failure earlier in the day blocks the all-clear even if the latest run recovered
        var earlierFail = Completed(0, now.AddHours(-10));
        earlierFail.Status = RunStatus.Failed;
        Assert.IsFalse(AlertNotificationService.AssessDigest(clean, new List<ScanRun> { clean, earlierFail }, now).IsAllClear);
    }

    // ---------- 12 / 13: delivery bookkeeping ----------

    [TestMethod]
    public async Task Digest_Success_SetsTimestamp_Failure_DoesNot()
    {
        var svc = new AlertNotificationService();
        var run = Completed();

        using (var ok = new TinyHttpServer(200))
        {
            var job = JobWithWebhook(ok.Url);
            Assert.IsTrue(await svc.DispatchDailyDigestAsync(job, run, new List<ScanRun> { run }));
            Assert.IsNotNull(job.AlertConfig.LastDailyDigestSentUtc);
            Assert.AreEqual(1, ok.RequestCount);
        }

        using (var broken = new TinyHttpServer(500))
        {
            var job = JobWithWebhook(broken.Url);
            Assert.IsFalse(await svc.DispatchDailyDigestAsync(job, run, new List<ScanRun> { run }), "HTTP 500 is a failed delivery");
            Assert.IsNull(job.AlertConfig.LastDailyDigestSentUtc);
        }

        var unreachable = JobWithWebhook("http://127.0.0.1:1/hook");
        Assert.IsFalse(await svc.DispatchDailyDigestAsync(unreachable, run, new List<ScanRun> { run }));
        Assert.IsNull(unreachable.AlertConfig.LastDailyDigestSentUtc);
    }

    [TestMethod]
    public async Task SendTestDailyDigest_ReportsRealOutcome_AndLeavesConfigUntouched()
    {
        var svc = new AlertNotificationService();
        var job = new ComparisonJob { Name = "t" };

        using (var ok = new TinyHttpServer(200))
        {
            var cfg = new JobAlertConfig { AzureWebhookUrl = ok.Url };
            var (success, message) = await svc.SendTestDailyDigestAsync(cfg, job);
            Assert.IsTrue(success, message);
            Assert.IsNull(cfg.LastDailyDigestSentUtc, "a test must not mark the real digest as sent");
        }

        using (var broken = new TinyHttpServer(503))
        {
            var (success, message) = await svc.SendTestDailyDigestAsync(new JobAlertConfig { AzureWebhookUrl = broken.Url }, job);
            Assert.IsFalse(success);
            StringAssert.Contains(message, "503");
        }
    }

    [TestMethod]
    public async Task FailureAlert_SetsLastAlertSent_OnlyAfterDelivery()
    {
        var svc = new AlertNotificationService();
        var run = Completed();
        run.Status = RunStatus.Failed;
        run.ErrorMessage = "boom";
        var reasons = new List<string> { "Scan failed with error: boom" };

        using (var broken = new TinyHttpServer(500))
        {
            var job = JobWithWebhook(broken.Url);
            Assert.IsFalse(await svc.DispatchAlertsAsync(job, run, reasons));
            Assert.IsNull(job.AlertConfig.LastAlertSentUtc, "nothing delivered => not marked as sent (it will be retried)");
        }

        using (var ok = new TinyHttpServer(200))
        {
            var job = JobWithWebhook(ok.Url);
            Assert.IsTrue(await svc.DispatchAlertsAsync(job, run, reasons));
            Assert.IsNotNull(job.AlertConfig.LastAlertSentUtc);
            Assert.AreEqual(DateTimeKind.Utc, job.AlertConfig.LastAlertSentUtc!.Value.Kind);
        }

        var noChannel = new ComparisonJob();
        noChannel.AlertConfig.IsEnabled = true;
        Assert.IsFalse(await svc.DispatchAlertsAsync(noChannel, run, reasons));
        Assert.IsNull(noChannel.AlertConfig.LastAlertSentUtc);
    }
}
