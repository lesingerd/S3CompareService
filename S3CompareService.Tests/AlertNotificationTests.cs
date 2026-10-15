using S3CompareService.Core.Models;
using S3CompareService.Core.Notifications;
using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

[TestClass]
public class AlertNotificationTests
{
    private string _testDbPath = string.Empty;
    private SqliteConnectionFactory _factory = null!;
    private JobRepository _jobRepo = null!;

    [TestInitialize]
    public async Task Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"s3alerts_{Guid.NewGuid():N}.db");
        _factory = new SqliteConnectionFactory($"Data Source={_testDbPath};Mode=ReadWriteCreate;");
        var migrator = new DatabaseMigrator(_factory);
        await migrator.MigrateAsync();
        _jobRepo = new JobRepository(_factory);
    }

    [TestCleanup]
    public void Cleanup()
    {
        try
        {
            if (File.Exists(_testDbPath)) File.Delete(_testDbPath);
            string shm = _testDbPath + "-shm";
            string wal = _testDbPath + "-wal";
            if (File.Exists(shm)) File.Delete(shm);
            if (File.Exists(wal)) File.Delete(wal);
        }
        catch { }
    }

    [TestMethod]
    public async Task JobRepository_PreservesAlertConfigCorrectly()
    {
        var job = new ComparisonJob
        {
            Name = "Alerts Persistence Test",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                MaxDisparitiesThreshold = 5,
                MaxMissingInBThreshold = 2,
                MaxMissingInAThreshold = 1,
                MaxSizeMismatchThreshold = 3,
                MaxEtagMismatchThreshold = 4,
                MaxSequenceGapsThreshold = 10,
                AlertOnScanFailure = true,
                NotifyOnCleanScan = false,
                SmtpHost = "smtp.office365.com",
                SmtpPort = 587,
                SmtpEnableSsl = true,
                SmtpUsername = "alerts@company.com",
                SmtpPassword = "SecretPassword123",
                FromEmail = "alerts@company.com",
                ToEmails = "sre@company.com, storage-team@company.com",
                AzureWebhookUrl = "https://prod-01.westus.logic.azure.com/workflows/...",
                AzureAppInsightsConnectionString = "InstrumentationKey=test-guid;"
            }
        };

        await _jobRepo.SaveAsync(job);

        var loaded = await _jobRepo.GetByIdAsync(job.Id);
        Assert.IsNotNull(loaded);
        Assert.IsNotNull(loaded.AlertConfig);
        Assert.IsTrue(loaded.AlertConfig.IsEnabled);
        Assert.AreEqual(5, loaded.AlertConfig.MaxDisparitiesThreshold);
        Assert.AreEqual(2, loaded.AlertConfig.MaxMissingInBThreshold);
        Assert.AreEqual("smtp.office365.com", loaded.AlertConfig.SmtpHost);
        Assert.AreEqual(587, loaded.AlertConfig.SmtpPort);
        Assert.AreEqual("alerts@company.com", loaded.AlertConfig.FromEmail);
        Assert.AreEqual("https://prod-01.westus.logic.azure.com/workflows/...", loaded.AlertConfig.AzureWebhookUrl);
    }

    [TestMethod]
    public async Task EvaluateAlert_DisabledConfig_DoesNotTrigger()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Disabled Alert Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = false,
                MaxDisparitiesThreshold = 0
            }
        };

        var run = new ScanRun
        {
            Status = RunStatus.Completed,
            FilteredDifferencesCount = 100
        };

        var (shouldAlert, reasons) = await service.EvaluateAlertConditionAsync(job, run);
        Assert.IsFalse(shouldAlert);
    }

    [TestMethod]
    public async Task EvaluateAlert_ScanFailure_TriggersWhenAlertOnScanFailureIsTrue()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Failure Alert Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                AlertOnFailure = true
            }
        };

        var run = new ScanRun
        {
            Status = RunStatus.Failed,
            ErrorMessage = "Connection reset by peer: s3.wasabisys.com"
        };

        var (shouldAlert, reasons) = await service.EvaluateAlertConditionAsync(job, run);
        Assert.IsTrue(shouldAlert);
        Assert.IsTrue(reasons.Any(r => r.Contains("failed", StringComparison.OrdinalIgnoreCase)));
    }

    [TestMethod]
    public async Task EvaluateAlert_InFlightReplicationDifferences_DoNotTriggerAlertIfConfirmedAreZero()
    {
        // Critical requirement: In-flight replication within grace period must NOT cause false alarms
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Replication Shield Job",
            ReplicationGracePeriodMinutes = 15,
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                MaxDisparitiesThreshold = 0 // Any confirmed disparity should alert
            }
        };

        var run = new ScanRun
        {
            Status = RunStatus.Completed,
            TotalDifferences = 25,
            FilteredDifferencesCount = 0, // 0 confirmed disparities!
            InFlightDifferencesCount = 25
        };

        var (shouldAlert, reasons) = await service.EvaluateAlertConditionAsync(job, run);

        // Should NOT trigger alert because confirmed disparities is 0!
        Assert.IsFalse(shouldAlert, "Alert should NOT fire for in-flight replication lag within grace period!");
    }

    [TestMethod]
    public async Task EvaluateAlert_ConfirmedDisparities_TriggersAlertWhenExceedingThreshold()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Disparity Threshold Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                MaxDisparitiesThreshold = 2
            }
        };

        var run = new ScanRun
        {
            Status = RunStatus.Completed,
            TotalDifferences = 5,
            FilteredDifferencesCount = 3,
            InFlightDifferencesCount = 2
        };

        var (shouldAlert, reasons) = await service.EvaluateAlertConditionAsync(job, run);

        Assert.IsTrue(shouldAlert);
        Assert.IsTrue(reasons.Any(r => r.Contains("Confirmed Disparities: 3")));
    }

    [TestMethod]
    public async Task EvaluateAlert_CleanScan_NotifyOnSuccess()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Digest Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                NotifyOnSuccess = true
            }
        };

        var run = new ScanRun
        {
            Status = RunStatus.Completed,
            FilteredDifferencesCount = 0,
            TotalDifferences = 0
        };

        var (shouldAlert, reasons) = await service.EvaluateAlertConditionAsync(job, run);

        Assert.IsTrue(shouldAlert);
        Assert.IsTrue(reasons.Any(r => r.Contains("Periodic status digest")));
    }

    [TestMethod]
    public async Task EvaluateDailyDigest_WhenDisabled_DoesNotTrigger()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Digest Test Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                SendDailyDigest = false
            }
        };

        var (shouldSend, _) = await service.EvaluateDailyDigestConditionAsync(job, new DateTime(2026, 10, 15, 12, 0, 0, DateTimeKind.Utc));
        Assert.IsFalse(shouldSend);
    }

    [TestMethod]
    public async Task EvaluateDailyDigest_WhenDueAndClean_Triggers()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Digest Due Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                SendDailyDigest = true,
                DailyDigestHourUtc = 8,
                OnlyIfNoRecentAlerts = true,
                LastAlertSentUtc = null,
                LastDailyDigestSentUtc = null
            }
        };

        // Scheduled for 8:00 UTC, current time is 9:00 UTC
        var now = new DateTime(2026, 10, 15, 9, 0, 0, DateTimeKind.Utc);
        var (shouldSend, reason) = await service.EvaluateDailyDigestConditionAsync(job, now);

        Assert.IsTrue(shouldSend);
        Assert.IsTrue(reason.Contains("Daily status report is due"));
    }

    [TestMethod]
    public async Task EvaluateDailyDigest_WhenAlreadySentToday_Suppressed()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Already Sent Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                SendDailyDigest = true,
                DailyDigestHourUtc = 8,
                LastDailyDigestSentUtc = new DateTime(2026, 10, 15, 8, 5, 0, DateTimeKind.Utc)
            }
        };

        // Same day, 10:00 UTC
        var now = new DateTime(2026, 10, 15, 10, 0, 0, DateTimeKind.Utc);
        var (shouldSend, reason) = await service.EvaluateDailyDigestConditionAsync(job, now);

        Assert.IsFalse(shouldSend);
        Assert.IsTrue(reason.Contains("already sent today"));
    }

    [TestMethod]
    public async Task EvaluateDailyDigest_WhenBeforeScheduledHour_Suppressed()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Before Hour Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                SendDailyDigest = true,
                DailyDigestHourUtc = 14 // 2pm UTC
            }
        };

        // Currently 9am UTC
        var now = new DateTime(2026, 10, 15, 9, 0, 0, DateTimeKind.Utc);
        var (shouldSend, reason) = await service.EvaluateDailyDigestConditionAsync(job, now);

        Assert.IsFalse(shouldSend);
        Assert.IsTrue(reason.Contains("has not reached scheduled digest hour"));
    }

    [TestMethod]
    public async Task EvaluateDailyDigest_WhenRecentAlertFired_Suppressed()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Recent Alert Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                SendDailyDigest = true,
                DailyDigestHourUtc = 8,
                OnlyIfNoRecentAlerts = true,
                // Alert fired 3 hours ago
                LastAlertSentUtc = new DateTime(2026, 10, 15, 7, 0, 0, DateTimeKind.Utc)
            }
        };

        var now = new DateTime(2026, 10, 15, 10, 0, 0, DateTimeKind.Utc);
        var (shouldSend, reason) = await service.EvaluateDailyDigestConditionAsync(job, now);

        Assert.IsFalse(shouldSend, "All-clear digest should be suppressed if an alert fired within the last 24 hours!");
        Assert.IsTrue(reason.Contains("Suppressed: An alert was triggered"));
    }

    [TestMethod]
    public async Task EvaluateDailyDigest_WhenRecentAlertFired_ButOnlyIfNoRecentAlertsIsFalse_Triggers()
    {
        var service = new AlertNotificationService();
        var job = new ComparisonJob
        {
            Name = "Digest Regardless Of Alert Job",
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                SendDailyDigest = true,
                DailyDigestHourUtc = 8,
                OnlyIfNoRecentAlerts = false, // Always send
                LastAlertSentUtc = new DateTime(2026, 10, 15, 7, 0, 0, DateTimeKind.Utc)
            }
        };

        var now = new DateTime(2026, 10, 15, 10, 0, 0, DateTimeKind.Utc);
        var (shouldSend, _) = await service.EvaluateDailyDigestConditionAsync(job, now);

        Assert.IsTrue(shouldSend);
    }
}
