namespace S3CompareService.Core.Models;

public class JobAlertConfig
{
    public bool IsEnabled { get; set; } = false;

    // Thresholds
    public bool AlertOnFailure { get; set; } = true;
    public bool AlertOnDisparities { get; set; } = true;

    /// <summary>
    /// Alert if confirmed disparities (excluding replication lag) exceed this threshold.
    /// Default is 0 (alert on any confirmed disparity).
    /// </summary>
    public int MaxDisparitiesThreshold { get; set; } = 0;

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
