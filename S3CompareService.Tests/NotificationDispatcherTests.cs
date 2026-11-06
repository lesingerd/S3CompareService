using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Core.Models;
using S3CompareService.Core.Notifications;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.Services;

namespace S3CompareService.Tests;

/// <summary>S-6: notifications are delivered on their own task, with timeouts, and never delay the scheduler or a scan.</summary>
[TestClass]
public class NotificationDispatcherTests : TestDbBase
{
    private sealed class FakeAlerts : IAlertNotificationService
    {
        public Func<Task<bool>> OnAlert = () => Task.FromResult(true);
        public Func<Task<bool>> OnDigest = () => Task.FromResult(true);
        public volatile bool DigestDue = true;
        public int AlertCalls, DigestCalls;

        public Task<(bool ShouldAlert, List<string> Reasons)> EvaluateAlertConditionAsync(ComparisonJob job, ScanRun run) => Task.FromResult((true, new List<string> { "x" }));
        public Task<bool> DispatchAlertsAsync(ComparisonJob job, ScanRun run, List<string> reasons, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref AlertCalls);
            return OnAlert();
        }

        public Task<(bool ShouldSend, string Reason)> EvaluateDailyDigestConditionAsync(ComparisonJob job, DateTime nowUtc) => Task.FromResult((DigestDue, ""));
        public Task<bool> DispatchDailyDigestAsync(ComparisonJob job, ScanRun? latestRun, List<ScanRun> recentRuns24h, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref DigestCalls);
            return OnDigest();
        }

        public Task<(bool Success, string Message)> SendTestEmailAsync(JobAlertConfig config, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<(bool Success, string Message)> SendTestAzureWebhookAsync(JobAlertConfig config, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<(bool Success, string Message)> SendTestDailyDigestAsync(JobAlertConfig config, ComparisonJob job, ScanRun? sampleRun = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<(bool Success, string Message)> TestSmtpConnectionAsync(JobAlertConfig config, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<(bool Success, string Message)> TestAzureWebhookAsync(JobAlertConfig config, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }

    /// <summary>The real alert service with the network calls replaced: SMTP hangs and ignores its token, the webhook answers at once.</summary>
    private sealed class HangingSmtpAlerts(NotificationOptions options) : AlertNotificationService(null, options)
    {
        public int WebhookCalls;
        public int SmtpCalls;

        protected override Task SendEmailAlertAsync(ComparisonJob job, ScanRun run, List<string> reasons, bool isFailure, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref SmtpCalls);
            return new TaskCompletionSource().Task; // never completes, never looks at the token
        }

        protected override Task SendAzureWebhookAlertAsync(ComparisonJob job, ScanRun run, List<string> reasons, bool isFailure, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref WebhookCalls);
            return Task.CompletedTask;
        }

        protected override Task TrackAppInsightsTelemetryAsync(ComparisonJob job, ScanRun run, bool isFailure, CancellationToken cancellationToken) => Task.CompletedTask;

        protected override Task SendDailyDigestEmailAsync(ComparisonJob job, ScanRun? latestRun, int totalRuns24h, int completedRuns24h, int failedRuns24h, DigestAssessment assessment, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref SmtpCalls);
            return new TaskCompletionSource().Task;
        }
    }

    private static readonly NotificationOptions FastOptions = new() { DeliveryTimeoutSeconds = 1 };

    private NotificationDispatcher NewDispatcher(IAlertNotificationService alerts) =>
        new(JobRepo, RunRepo, alerts, FastOptions, NullLogger<NotificationDispatcher>.Instance);

    private static ComparisonJob DigestJob(string id = "j1") => new()
    {
        Id = id,
        Name = "digest job",
        IsEnabled = true,
        ScheduleType = JobScheduleType.IntervalMinutes,
        IntervalMinutes = 1440,
        LastRunTimeUtc = DateTime.UtcNow, // not due, so the scheduler does not start a scan
        BucketA = { BucketName = "a" },
        BucketB = { BucketName = "b" },
        AlertConfig = { IsEnabled = true, SendDailyDigest = true, DailyDigestHourUtc = 0, SmtpHost = "smtp.invalid", ToEmails = "x@example.com" }
    };

    private static ScanRun FailedRun(string jobId) => new() { Id = Guid.NewGuid().ToString(), JobId = jobId, Status = RunStatus.Failed, StartTimeUtc = DateTime.UtcNow };

    private static async Task WaitForAsync(Func<bool> condition, int timeoutMs = 8000)
    {
        var sw = Stopwatch.StartNew();
        while (!condition())
        {
            if (sw.ElapsedMilliseconds > timeoutMs) Assert.Fail("Timed out waiting for the condition.");
            await Task.Delay(20);
        }
    }

    private static async Task<T> RunningAsync<T>(NotificationDispatcher dispatcher, Func<Task<T>> body)
    {
        await dispatcher.StartAsync(CancellationToken.None);
        try { return await body(); }
        finally { await dispatcher.StopAsync(CancellationToken.None); }
    }

    // --- the scheduler loop ---

    [TestMethod]
    public async Task SchedulerLoop_IsNotDelayed_ByADigestThatHangs()
    {
        var job = DigestJob();
        await JobRepo.SaveAsync(job);
        var alerts = new FakeAlerts { OnDigest = () => new TaskCompletionSource<bool>().Task }; // a send that never returns
        var dispatcher = NewDispatcher(alerts);
        var coordinator = new ScanExecutionCoordinator(JobRepo, RunRepo, DiffRepo, AuditRepo, alerts, NullLogger<ScanExecutionCoordinator>.Instance, notifications: dispatcher);
        var services = new ServiceCollection()
            .AddSingleton<IJobRepository>(JobRepo).AddSingleton<IScanRunRepository>(RunRepo)
            .AddSingleton<IAlertNotificationService>(alerts).AddSingleton(coordinator)
            .AddSingleton<INotificationQueue>(dispatcher).BuildServiceProvider();
        var scheduler = new ScanSchedulerService(services, NullLogger<ScanSchedulerService>.Instance);

        await RunningAsync(dispatcher, async () =>
        {
            // Every pass must finish (a pass that waited for the hung send would hit the 10 s guard) while the digest send is still pending.
            for (int i = 0; i < 5; i++)
            {
                using var guard = new CancellationTokenSource(10000);
                await scheduler.EvaluateSchedulesAsync(guard.Token);
                if (i == 0) await WaitForAsync(() => alerts.DigestCalls == 1);
            }

            Assert.AreEqual(1, alerts.DigestCalls, "the digest is queued once, not again at every pass, and its send is still hanging");
            Assert.AreEqual(0, dispatcher.GetStatus().Delivered + dispatcher.GetStatus().Failed);
            return 0;
        });
    }

    [TestMethod]
    public async Task EnqueueAlert_ReturnsAtOnce_AndDeliversNothingInline()
    {
        var alerts = new FakeAlerts { OnAlert = () => new TaskCompletionSource<bool>().Task };
        var dispatcher = NewDispatcher(alerts);
        dispatcher.EnqueueAlert(new ComparisonJob { Id = "j" }, FailedRun("j"), new List<string> { "Scan failed" });
        Assert.AreEqual(1, dispatcher.GetStatus().Queued, "enqueue returns at once and nothing was delivered inline");
        Assert.AreEqual(0, alerts.AlertCalls);
        await Task.CompletedTask;
    }

    // --- timeouts ---

    [TestMethod]
    public async Task DeliveryTimeout_AbandonsAChannelThatIgnoresItsToken_AndTheOthersStillDeliver()
    {
        var service = new HangingSmtpAlerts(FastOptions);
        var job = new ComparisonJob
        {
            Id = "j", Name = "n",
            AlertConfig = { IsEnabled = true, SmtpHost = "smtp.invalid", ToEmails = "x@example.com", AzureWebhookUrl = "https://example.invalid/hook", AzureAppInsightsConnectionString = "InstrumentationKey=00000000-0000-0000-0000-000000000000" }
        };

        var sw = Stopwatch.StartNew();
        bool delivered = await service.DispatchAlertsAsync(job, FailedRun("j"), new List<string> { "Scan failed" });
        sw.Stop();

        Assert.IsTrue(delivered, "the webhook delivered although SMTP hung");
        Assert.AreEqual(1, service.WebhookCalls);
        Assert.AreEqual(1, service.SmtpCalls);
        Assert.IsTrue(sw.ElapsedMilliseconds >= 900 && sw.ElapsedMilliseconds < 5000, $"the call waited for the 1 s timeout and no longer ({sw.ElapsedMilliseconds} ms)");
    }

    [TestMethod]
    public async Task DailyDigest_WithOnlyAHungSmtp_ReturnsFalseAfterTheTimeout()
    {
        var service = new HangingSmtpAlerts(FastOptions);
        var job = DigestJob();

        var sw = Stopwatch.StartNew();
        bool delivered = await service.DispatchDailyDigestAsync(job, null, new List<ScanRun>());

        Assert.IsFalse(delivered);
        Assert.IsTrue(sw.ElapsedMilliseconds < 5000, $"{sw.ElapsedMilliseconds} ms");
        Assert.IsNull(job.AlertConfig.LastDailyDigestSentUtc, "no success, no timestamp");
    }

    [TestMethod]
    public void DeliveryTimeoutSetting_IsReadAndFallsBackToTheDefault()
    {
        IConfiguration Config(string? value) => new ConfigurationBuilder()
            .AddInMemoryCollection(value is null ? new Dictionary<string, string?>() : new() { ["S3Compare:Notifications:DeliveryTimeoutSeconds"] = value })
            .Build();

        Assert.AreEqual(TimeSpan.FromSeconds(30), S3CompareService.Worker.HardeningSettings.Notifications(Config(null)).Timeout);
        Assert.AreEqual(TimeSpan.FromSeconds(7), S3CompareService.Worker.HardeningSettings.Notifications(Config("7")).Timeout);
        Assert.AreEqual(TimeSpan.FromSeconds(30), S3CompareService.Worker.HardeningSettings.Notifications(Config("0")).Timeout);
        Assert.AreEqual(TimeSpan.FromSeconds(30), S3CompareService.Worker.HardeningSettings.Notifications(Config("abc")).Timeout);
    }

    // --- timestamps and dedupe ---

    [TestMethod]
    public async Task AlertTimestamp_IsWrittenOnlyAfterASuccessfulDelivery()
    {
        await JobRepo.SaveAsync(DigestJob("a1"));
        var alerts = new FakeAlerts { OnAlert = () => Task.FromResult(false) };
        var dispatcher = NewDispatcher(alerts);

        await RunningAsync(dispatcher, async () =>
        {
            dispatcher.EnqueueAlert((await JobRepo.GetByIdAsync("a1"))!, FailedRun("a1"), new List<string> { "Scan failed" });
            await WaitForAsync(() => dispatcher.GetStatus().Failed == 1);
            Assert.IsNull((await JobRepo.GetByIdAsync("a1"))!.AlertConfig.LastAlertSentUtc, "a failed delivery leaves the timestamp alone");

            alerts.OnAlert = () => Task.FromResult(true);
            dispatcher.EnqueueAlert((await JobRepo.GetByIdAsync("a1"))!, FailedRun("a1"), new List<string> { "Scan failed" });
            await WaitForAsync(() => dispatcher.GetStatus().Delivered == 1);
            Assert.IsNotNull((await JobRepo.GetByIdAsync("a1"))!.AlertConfig.LastAlertSentUtc);
            return 0;
        });
    }

    [TestMethod]
    public async Task DigestTimestamp_IsWrittenOnlyAfterASuccessfulDelivery_AndAFailureDoesNotRetryAtOnce()
    {
        await JobRepo.SaveAsync(DigestJob("d1"));
        var alerts = new FakeAlerts { OnDigest = () => Task.FromResult(false) };
        var dispatcher = NewDispatcher(alerts);
        var now = DateTime.UtcNow;

        await RunningAsync(dispatcher, async () =>
        {
            Assert.IsTrue(dispatcher.TryEnqueueDigest("d1", now));
            await WaitForAsync(() => dispatcher.GetStatus().Failed == 1);
            Assert.IsNull((await JobRepo.GetByIdAsync("d1"))!.AlertConfig.LastDailyDigestSentUtc);
            Assert.IsFalse(dispatcher.TryEnqueueDigest("d1", now), "a failed digest waits before it is queued again, so a dead server is not hit every 10 s");
            return 0;
        });

        alerts.OnDigest = () => Task.FromResult(true);
        var second = NewDispatcher(alerts); // a new dispatcher has no retry delay
        await RunningAsync(second, async () =>
        {
            Assert.IsTrue(second.TryEnqueueDigest("d1", now));
            await WaitForAsync(() => second.GetStatus().Delivered == 1);
            Assert.IsNotNull((await JobRepo.GetByIdAsync("d1"))!.AlertConfig.LastDailyDigestSentUtc);
            return 0;
        });
    }

    [TestMethod]
    public async Task Digest_IsQueuedOncePerJobAndDay()
    {
        await JobRepo.SaveAsync(DigestJob("d2"));
        await JobRepo.SaveAsync(DigestJob("d3"));
        var dispatcher = NewDispatcher(new FakeAlerts());
        var now = DateTime.UtcNow;

        Assert.IsTrue(dispatcher.TryEnqueueDigest("d2", now));
        Assert.IsFalse(dispatcher.TryEnqueueDigest("d2", now.AddMinutes(1)), "already queued today");
        Assert.AreEqual(1, dispatcher.GetStatus().Queued);
        Assert.IsTrue(dispatcher.TryEnqueueDigest("d3", now), "another job");

        await RunningAsync(dispatcher, async () =>
        {
            await WaitForAsync(() => dispatcher.GetStatus().Queued == 0 && dispatcher.GetStatus().Delivered + dispatcher.GetStatus().Failed >= 2);
            Assert.IsFalse(dispatcher.TryEnqueueDigest("d2", now), "a digest that was handled today is not queued again");
            return 0;
        });

        Assert.IsTrue(NewDispatcher(new FakeAlerts()).TryEnqueueDigest("d2", now.AddDays(1)), "another day starts afresh");
    }

    [TestMethod]
    public async Task Digest_IsNotSent_WhenTheConditionNoLongerHolds()
    {
        await JobRepo.SaveAsync(DigestJob("d4"));
        var alerts = new FakeAlerts { DigestDue = false }; // e.g. an alert went out since the scheduler looked ("only if no recent alerts")
        var dispatcher = NewDispatcher(alerts);

        await RunningAsync(dispatcher, async () =>
        {
            Assert.IsTrue(dispatcher.TryEnqueueDigest("d4", DateTime.UtcNow));
            await WaitForAsync(() => dispatcher.TryEnqueueDigest("d4", DateTime.UtcNow)); // true once the key is released: it can be queued again when the condition holds
            await Task.Delay(200);
            Assert.AreEqual(0, alerts.DigestCalls);
            return 0;
        });
    }

    // --- the bounded queue ---

    [TestMethod]
    public void FullQueue_DropsTheOldestDigest_AndCountsIt()
    {
        var dispatcher = NewDispatcher(new FakeAlerts());
        var now = DateTime.UtcNow;
        for (int i = 0; i < NotificationDispatcher.Capacity; i++) Assert.IsTrue(dispatcher.TryEnqueueDigest($"job{i}", now));

        dispatcher.EnqueueAlert(new ComparisonJob { Id = "j" }, FailedRun("j"), new List<string> { "Scan failed" });

        var status = dispatcher.GetStatus();
        Assert.AreEqual(NotificationDispatcher.Capacity, status.Queued);
        Assert.AreEqual(1, status.Dropped);
    }

    [TestMethod]
    public void FullQueue_NeverDropsFailureAlerts()
    {
        var dispatcher = NewDispatcher(new FakeAlerts());
        for (int i = 0; i < NotificationDispatcher.Capacity; i++)
            dispatcher.EnqueueAlert(new ComparisonJob { Id = $"j{i}" }, FailedRun($"j{i}"), new List<string> { "Scan failed" });

        Assert.IsFalse(dispatcher.TryEnqueueDigest("late", DateTime.UtcNow), "a digest does not push out an alert");
        dispatcher.EnqueueAlert(new ComparisonJob { Id = "extra" }, FailedRun("extra"), new List<string> { "Confirmed Disparities: 5" });

        var status = dispatcher.GetStatus();
        Assert.AreEqual(NotificationDispatcher.Capacity + 1, status.Queued, "the alert is kept even above the capacity");
        Assert.AreEqual(1, status.Dropped, "only the digest was dropped");
    }

    [TestMethod]
    public void DroppedDigest_CanBeQueuedAgain()
    {
        var dispatcher = NewDispatcher(new FakeAlerts());
        var now = DateTime.UtcNow;
        for (int i = 0; i < NotificationDispatcher.Capacity; i++) dispatcher.TryEnqueueDigest($"job{i}", now);
        dispatcher.TryEnqueueDigest("newest", now); // pushes job0 out

        Assert.AreEqual(1, dispatcher.GetStatus().Dropped);
        Assert.IsTrue(dispatcher.TryEnqueueDigest("job0", now), "the dropped digest's key was released");
    }

    // --- resilience ---

    [TestMethod]
    public async Task Dispatcher_SurvivesExceptions_AndRecordsThem()
    {
        await JobRepo.SaveAsync(DigestJob("e1"));
        int n = 0;
        var alerts = new FakeAlerts { OnAlert = () => Interlocked.Increment(ref n) == 1 ? throw new InvalidOperationException("boom") : Task.FromResult(true) };
        var dispatcher = NewDispatcher(alerts);

        await RunningAsync(dispatcher, async () =>
        {
            dispatcher.EnqueueAlert((await JobRepo.GetByIdAsync("e1"))!, FailedRun("e1"), new List<string> { "Scan failed" });
            await WaitForAsync(() => dispatcher.GetStatus().Failed == 1);
            var failing = dispatcher.GetStatus();
            Assert.AreEqual("boom", failing.LastError);
            Assert.AreEqual(1, failing.ConsecutiveFailures);
            Assert.IsNotNull(failing.FailingSinceUtc);

            dispatcher.EnqueueAlert((await JobRepo.GetByIdAsync("e1"))!, FailedRun("e1"), new List<string> { "Scan failed" });
            await WaitForAsync(() => dispatcher.GetStatus().Delivered == 1);
            var recovered = dispatcher.GetStatus();
            Assert.AreEqual(0, recovered.ConsecutiveFailures);
            Assert.IsNull(recovered.FailingSinceUtc);
            Assert.IsNotNull(recovered.LastSuccessUtc);
            return 0;
        });
    }

    [TestMethod]
    public async Task Dispatcher_AbandonsADispatchThatIgnoresItsToken_AndKeepsGoing()
    {
        await JobRepo.SaveAsync(DigestJob("h1"));
        int n = 0;
        var alerts = new FakeAlerts { OnAlert = () => Interlocked.Increment(ref n) == 1 ? new TaskCompletionSource<bool>().Task : Task.FromResult(true) };
        var dispatcher = NewDispatcher(alerts); // backstop = 1 s timeout + 5 s

        await RunningAsync(dispatcher, async () =>
        {
            dispatcher.EnqueueAlert((await JobRepo.GetByIdAsync("h1"))!, FailedRun("h1"), new List<string> { "Scan failed" });
            dispatcher.EnqueueAlert((await JobRepo.GetByIdAsync("h1"))!, FailedRun("h1"), new List<string> { "Scan failed" });
            await WaitForAsync(() => dispatcher.GetStatus().Delivered == 1, 15000);
            Assert.AreEqual(1, dispatcher.GetStatus().Failed, "the hung dispatch counted as a failure");
            return 0;
        });
    }
}
