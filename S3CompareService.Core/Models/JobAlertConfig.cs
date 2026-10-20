namespace S3CompareService.Core.Models;

public class JobAlertConfig
{
    public bool IsEnabled { get; set; } = false;

    // Thresholds
    public bool AlertOnFailure { get; set; } = true;
    public bool AlertOnDisparities { get; set; } = true;

    /// <summary>
    /// Alert if confirmed disparities (excluding replication lag) exceed this threshold.
    /// Default is 0 (alert on any confirmed disparity). Unlike the per-type thresholds below, 0 here is NOT "disabled".
    /// </summary>
    public int MaxDisparitiesThreshold { get; set; } = 0;

    // Per-type thresholds: 0 (or less) = DISABLED for that type; otherwise alert when the confirmed count for the
    // type is strictly greater than the threshold. An alert fires if the overall threshold above OR any enabled
    // per-type threshold is exceeded.
    public int MaxMissingInBThreshold { get; set; } = 0;
    public int MaxMissingInAThreshold { get; set; } = 0;
    public int MaxSizeMismatchThreshold { get; set; } = 0;
    public int MaxETagMismatchThreshold { get; set; } = 0;
    public int MaxContentMismatchThreshold { get; set; } = 0;
    public int MaxSequenceGapThreshold { get; set; } = 0;

    /// <summary>
    /// If true, sends a notification even when no thresholds are breached (digest/heartbeat).
    /// </summary>
    public bool NotifyOnSuccess { get; set; } = false;

    // Daily Status Report / All-Clear Confirmation
    /// <summary>
    /// If enabled, sends a daily status report ("all-clear") with metrics confirmation that the service is running.
    /// </summary>
    public bool SendDailyDigest { get; set; } = false;

    /// <summary>
    /// The hour of the day (0-23 UTC) when the daily status report should be dispatched. Default is 8 (08:00 UTC).
    /// </summary>
    public int DailyDigestHourUtc { get; set; } = 8;

    /// <summary>
    /// If true, the daily status report is ONLY sent if no disparity or failure alerts have triggered recently (last 24 hours).
    /// </summary>
    public bool OnlyIfNoRecentAlerts { get; set; } = true;

    /// <summary>
    /// Timestamp (UTC) when an alert (threshold breach or failure) was last sent.
    /// </summary>
    public DateTime? LastAlertSentUtc { get; set; }

    /// <summary>
    /// Timestamp (UTC) when a daily status digest was last sent.
    /// </summary>
    public DateTime? LastDailyDigestSentUtc { get; set; }

    // SMTP Settings
    public string SmtpHost { get; set; } = string.Empty;
    public int SmtpPort { get; set; } = 587;
    public bool SmtpUseSsl { get; set; } = true;
    public string SmtpUsername { get; set; } = string.Empty;
    public string SmtpPassword { get; set; } = string.Empty;
    public string FromEmail { get; set; } = string.Empty;
    public string ToEmails { get; set; } = string.Empty;
    public string EmailSubjectPrefix { get; set; } = "[S3Compare Alert]";

    // Azure Alerting Settings
    /// <summary>
    /// Generic Webhook URL for Azure Logic Apps, Azure Functions, or Azure Monitor Action Groups.
    /// </summary>
    public string AzureWebhookUrl { get; set; } = string.Empty;

    /// <summary>
    /// Optional Azure Application Insights Connection String to emit custom metric telemetry.
    /// </summary>
    public string AzureAppInsightsConnectionString { get; set; } = string.Empty;

    // Aliases for UI & Test bindings
    [System.Text.Json.Serialization.JsonIgnore]
    public bool AlertOnScanFailure { get => AlertOnFailure; set => AlertOnFailure = value; }

    [System.Text.Json.Serialization.JsonIgnore]
    public bool NotifyOnCleanScan { get => NotifyOnSuccess; set => NotifyOnSuccess = value; }

    [System.Text.Json.Serialization.JsonIgnore]
    public bool SmtpEnableSsl { get => SmtpUseSsl; set => SmtpUseSsl = value; }

    [System.Text.Json.Serialization.JsonIgnore]
    public int MaxEtagMismatchThreshold { get => MaxETagMismatchThreshold; set => MaxETagMismatchThreshold = value; }

    [System.Text.Json.Serialization.JsonIgnore]
    public int MaxSequenceGapsThreshold { get => MaxSequenceGapThreshold; set => MaxSequenceGapThreshold = value; }
}
