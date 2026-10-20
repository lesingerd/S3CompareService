using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Mail;
using System.Text;
using System.Text.Json;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Core.Notifications;

public interface IAlertNotificationService
{
    Task<(bool ShouldAlert, List<string> Reasons)> EvaluateAlertConditionAsync(ComparisonJob job, ScanRun run);
    Task<bool> DispatchAlertsAsync(ComparisonJob job, ScanRun run, List<string> reasons, CancellationToken cancellationToken = default);
    Task<(bool ShouldSend, string Reason)> EvaluateDailyDigestConditionAsync(ComparisonJob job, DateTime nowUtc);
    Task<bool> DispatchDailyDigestAsync(ComparisonJob job, ScanRun? latestRun, List<ScanRun> recentRuns24h, CancellationToken cancellationToken = default);
    Task<(bool Success, string Message)> SendTestEmailAsync(JobAlertConfig config, CancellationToken cancellationToken = default);
    Task<(bool Success, string Message)> SendTestAzureWebhookAsync(JobAlertConfig config, CancellationToken cancellationToken = default);
    Task<(bool Success, string Message)> SendTestDailyDigestAsync(JobAlertConfig config, ComparisonJob job, ScanRun? sampleRun = null, CancellationToken cancellationToken = default);
    Task<(bool Success, string Message)> TestSmtpConnectionAsync(JobAlertConfig config, CancellationToken cancellationToken = default);
    Task<(bool Success, string Message)> TestAzureWebhookAsync(JobAlertConfig config, CancellationToken cancellationToken = default);
}

public class AlertNotificationService : IAlertNotificationService
{
    private readonly IAuditLogRepository? _auditRepo;
    private static readonly HttpClient HttpClient = new() { Timeout = TimeSpan.FromSeconds(15) };

    public AlertNotificationService(IAuditLogRepository? auditRepo = null)
    {
        _auditRepo = auditRepo;
    }

    public Task<(bool ShouldAlert, List<string> Reasons)> EvaluateAlertConditionAsync(ComparisonJob job, ScanRun run)
    {
        var cfg = job.AlertConfig;
        var reasons = new List<string>();

        if (!cfg.IsEnabled)
        {
            return Task.FromResult((false, reasons));
        }

        // 1. Scan Execution Failure
        if (run.Status == RunStatus.Failed && cfg.AlertOnFailure)
        {
            reasons.Add($"Scan failed with error: {run.ErrorMessage ?? "Unknown failure"}");
        }

        // 2. Disparity Thresholds (Evaluated against CONFIRMED disparities to shield replication lag)
        // Overall threshold: confirmed disparities strictly above it alert (0 = alert on any confirmed disparity).
        // Per-type thresholds: 0 (or less) = disabled; an enabled threshold alerts when its count is strictly above it.
        // An alert fires if the overall threshold OR any enabled per-type threshold is exceeded.
        if (cfg.AlertOnDisparities)
        {
            if (run.FilteredDifferencesCount > cfg.MaxDisparitiesThreshold && run.FilteredDifferencesCount > 0)
            {
                reasons.Add($"Confirmed Disparities: {run.FilteredDifferencesCount:N0} (Exceeds threshold of {cfg.MaxDisparitiesThreshold:N0})");
            }
            AddIfExceeded(reasons, "Missing in Bucket B", run.MissingInBucketBCount, cfg.MaxMissingInBThreshold);
            AddIfExceeded(reasons, "Missing in Bucket A", run.MissingInBucketACount, cfg.MaxMissingInAThreshold);
            AddIfExceeded(reasons, "Size Mismatches", run.SizeMismatchCount, cfg.MaxSizeMismatchThreshold);
            AddIfExceeded(reasons, "ETag Mismatches", run.ETagMismatchCount, cfg.MaxETagMismatchThreshold);
            AddIfExceeded(reasons, "Content Byte Mismatches", run.ContentMismatchCount, cfg.MaxContentMismatchThreshold);
            AddIfExceeded(reasons, "Sequence Gaps", run.SequenceGapCount, cfg.MaxSequenceGapThreshold);
        }

        // 3. Digest on Success
        if (reasons.Count == 0 && cfg.NotifyOnSuccess && run.Status == RunStatus.Completed)
        {
            reasons.Add("Periodic status digest: All metrics healthy, within thresholds.");
            return Task.FromResult((true, reasons));
        }

        bool shouldAlert = reasons.Count > 0;
        return Task.FromResult((shouldAlert, reasons));
    }

    private static void AddIfExceeded(List<string> reasons, string label, long count, int threshold)
    {
        if (threshold > 0 && count > threshold)
        {
            reasons.Add($"{label}: {count:N0} (Exceeds threshold of {threshold:N0})");
        }
    }

    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);

    private static CancellationTokenSource CreateDeliveryCts(CancellationToken cancellationToken)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(DeliveryTimeout);
        return cts;
    }

    private static async Task PostJsonAsync(string url, object payload, CancellationToken cancellationToken)
    {
        using var cts = CreateDeliveryCts(cancellationToken);
        using var resp = await HttpClient.PostAsJsonAsync(url, payload, cts.Token).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Dispatches threshold / failure alerts. Returns true when at least one channel (SMTP or webhook) delivered.
    /// <c>AlertConfig.LastAlertSentUtc</c> is updated on the passed job only after a channel succeeded; callers
    /// should persist it with <c>IJobRepository.UpdateAlertTimestampsAsync</c>.
    /// </summary>
    public async Task<bool> DispatchAlertsAsync(ComparisonJob job, ScanRun run, List<string> reasons, CancellationToken cancellationToken = default)
    {
        var cfg = job.AlertConfig;
        if (!cfg.IsEnabled) return false;

        bool isFailure = run.Status == RunStatus.Failed || (reasons.Count > 0 && !reasons[0].StartsWith("Periodic status"));
        bool anyDelivered = false;

        // 1. Dispatch Email via SMTP
        if (!string.IsNullOrWhiteSpace(cfg.SmtpHost) && !string.IsNullOrWhiteSpace(cfg.ToEmails))
        {
            try
            {
                using var cts = CreateDeliveryCts(cancellationToken);
                await SendEmailAlertAsync(job, run, reasons, isFailure, cts.Token).ConfigureAwait(false);
                anyDelivered = true;
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("INFO", "Alerts", $"SMTP alert sent successfully to {cfg.ToEmails} for job '{job.Name}'.", scanRunId: run.Id, jobId: job.Id, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("ERROR", "Alerts", $"Failed to send SMTP alert to {cfg.ToEmails}: {ex.Message}", details: ex.StackTrace, scanRunId: run.Id, jobId: job.Id, cancellationToken: CancellationToken.None);
                }
            }
        }

        // 2. Dispatch Azure Webhook (Logic Apps / Action Groups / Azure Functions)
        if (!string.IsNullOrWhiteSpace(cfg.AzureWebhookUrl))
        {
            try
            {
                await SendAzureWebhookAlertAsync(job, run, reasons, isFailure, cancellationToken).ConfigureAwait(false);
                anyDelivered = true;
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("INFO", "Alerts", $"Azure Webhook alert dispatched successfully for job '{job.Name}'.", scanRunId: run.Id, jobId: job.Id, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("ERROR", "Alerts", $"Failed to dispatch Azure Webhook alert: {ex.Message}", details: ex.StackTrace, scanRunId: run.Id, jobId: job.Id, cancellationToken: CancellationToken.None);
                }
            }
        }

        // 3. Dispatch to Azure Application Insights Telemetry (telemetry only: does not count as an alert delivery)
        if (!string.IsNullOrWhiteSpace(cfg.AzureAppInsightsConnectionString))
        {
            try
            {
                await TrackAppInsightsTelemetryAsync(job, run, isFailure, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("WARN", "Alerts", $"Failed to post App Insights telemetry: {ex.Message}", scanRunId: run.Id, jobId: job.Id, cancellationToken: CancellationToken.None);
                }
            }
        }

        if (isFailure && anyDelivered)
        {
            cfg.LastAlertSentUtc = DateTime.UtcNow;
        }

        return anyDelivered;
    }

    private async Task SendEmailAlertAsync(ComparisonJob job, ScanRun run, List<string> reasons, bool isFailure, CancellationToken cancellationToken)
    {
        var cfg = job.AlertConfig;
        using var client = new SmtpClient(cfg.SmtpHost.Trim(), cfg.SmtpPort)
        {
            EnableSsl = cfg.SmtpUseSsl,
            DeliveryMethod = SmtpDeliveryMethod.Network,
            Timeout = 15000
        };

        if (!string.IsNullOrWhiteSpace(cfg.SmtpUsername))
        {
            client.Credentials = new NetworkCredential(cfg.SmtpUsername.Trim(), cfg.SmtpPassword);
        }

        string from = string.IsNullOrWhiteSpace(cfg.FromEmail) ? "s3compareservice@localhost" : cfg.FromEmail.Trim();
        string subject = $"{cfg.EmailSubjectPrefix} {(isFailure ? "DISPARITY ALERT" : "AUDIT DIGEST")} - Job '{job.Name}' - {run.Status}";

        using var message = new MailMessage
        {
            From = new MailAddress(from, "S3 Compare Service"),
            Subject = subject,
            IsBodyHtml = true,
            Body = BuildHtmlEmailBody(job, run, reasons, isFailure)
        };

        var recipients = cfg.ToEmails.Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var r in recipients)
        {
            message.To.Add(r.Trim());
        }

        await client.SendMailAsync(message, cancellationToken).ConfigureAwait(false);
    }

    private static string BuildHtmlEmailBody(ComparisonJob job, ScanRun run, List<string> reasons, bool isFailure)
    {
        string headerBg = isFailure ? "#C62828" : "#2E7D32";
        string headerTitle = isFailure ? "⚠️ S3 DISPARITY ALERT: THRESHOLD BREACHED" : "✅ S3 BUCKET AUDIT SUMMARY";

        var sb = new StringBuilder();
        sb.AppendLine($@"<!DOCTYPE html>
<html>
<head>
<style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f4f6f9; color: #333333; margin: 0; padding: 20px; }}
    .container {{ max-width: 650px; margin: 0 auto; background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.1); border: 1px solid #e1e4e8; }}
    .header {{ background-color: {headerBg}; color: #ffffff; padding: 20px 24px; }}
    .header h1 {{ margin: 0; font-size: 18px; letter-spacing: 0.5px; }}
    .content {{ padding: 24px; }}
    .alert-box {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 12px 16px; margin-bottom: 20px; border-radius: 4px; }}
    .alert-box.danger {{ background-color: #f8d7da; border-left-color: #dc3545; }}
    .table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
    .table th, .table td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #e9ecef; font-size: 13px; }}
    .table th {{ background-color: #f8f9fa; font-weight: 600; color: #495057; }}
    .badge {{ display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }}
    .badge-danger {{ background: #ff4d4f; color: #ffffff; }}
    .badge-warning {{ background: #faad14; color: #ffffff; }}
    .badge-success {{ background: #52c41a; color: #ffffff; }}
    .footer {{ background: #f8f9fa; padding: 16px 24px; font-size: 11px; color: #6c757d; border-top: 1px solid #e9ecef; text-align: center; }}
</style>
</head>
<body>
<div class='container'>
    <div class='header'>
        <h1>{headerTitle}</h1>
        <div style='font-size: 13px; opacity: 0.9; margin-top: 4px;'>Job: <strong>{WebUtility.HtmlEncode(job.Name)}</strong> | Run ID: {run.Id[..8]}</div>
    </div>
    <div class='content'>");

        if (reasons.Count > 0)
        {
            string boxClass = isFailure ? "alert-box danger" : "alert-box";
            sb.AppendLine($"<div class='{boxClass}'><strong>Trigger Reasons:</strong><ul>");
            foreach (var r in reasons)
            {
                sb.AppendLine($"<li>{WebUtility.HtmlEncode(r)}</li>");
            }
            sb.AppendLine("</ul></div>");
        }

        sb.AppendLine($@"
        <table class='table'>
            <tr><th colspan='2'>Scan Execution Summary</th></tr>
            <tr><td><strong>Status</strong></td><td><span class='badge {(run.Status == RunStatus.Completed ? (run.FilteredDifferencesCount > 0 ? "badge-danger" : "badge-success") : "badge-danger")}'>{run.Status}</span></td></tr>
            <tr><td><strong>Start Time (UTC)</strong></td><td>{run.StartTimeUtc:yyyy-MM-dd HH:mm:ss} UTC</td></tr>
            <tr><td><strong>Duration</strong></td><td>{run.FormattedDuration} ({run.DurationSeconds:F1}s)</td></tr>
            <tr><td><strong>Objects Evaluated</strong></td><td>{run.ObjectsCompared:N0}</td></tr>
            <tr><td><strong>Matched Identical</strong></td><td>{run.MatchedCount:N0}</td></tr>
            <tr><td><strong>Confirmed Disparities</strong></td><td><strong style='color: {(run.FilteredDifferencesCount > 0 ? "#dc3545" : "#28a745")};'>{run.FilteredDifferencesCount:N0}</strong></td></tr>
            <tr><td><strong>In-Flight Replication Lag</strong></td><td>{run.InFlightDifferencesCount:N0} <span style='font-size: 11px; color: #888888;'>(shielded by {run.ReplicationGracePeriodMinutes}m grace window)</span></td></tr>
            <tr><td><strong>Total Raw Differences</strong></td><td>{run.TotalDifferences:N0}</td></tr>
        </table>

        <table class='table'>
            <tr><th colspan='2'>Bucket Configuration</th></tr>
            <tr><td><strong>Bucket A (Source)</strong></td><td>{WebUtility.HtmlEncode(job.BucketA.BucketName)} @ {WebUtility.HtmlEncode(job.BucketA.EndpointUrl)}</td></tr>
            <tr><td><strong>Bucket B (Target)</strong></td><td>{WebUtility.HtmlEncode(job.BucketB.BucketName)} @ {WebUtility.HtmlEncode(job.BucketB.EndpointUrl)}</td></tr>
        </table>
    </div>
    <div class='footer'>
        Automated alert generated by <strong>S3CompareService</strong>. Continuous audit and replication shield.
    </div>
</div>
</body>
</html>");

        return sb.ToString();
    }

    private async Task SendAzureWebhookAlertAsync(ComparisonJob job, ScanRun run, List<string> reasons, bool isFailure, CancellationToken cancellationToken)
    {
        var cfg = job.AlertConfig;
        var payload = new
        {
            alertType = isFailure ? "S3DisparityThresholdBreach" : "S3AuditStatusDigest",
            severity = isFailure ? "High" : "Informational",
            timestampUtc = DateTime.UtcNow.ToString("O"),
            jobId = job.Id,
            jobName = job.Name,
            runId = run.Id,
            status = run.Status.ToString(),
            isThresholdBreached = isFailure,
            breachReasons = reasons,
            metrics = new
            {
                objectsEvaluated = run.ObjectsCompared,
                matchedCount = run.MatchedCount,
                confirmedDisparities = run.FilteredDifferencesCount,
                inFlightReplicationLag = run.InFlightDifferencesCount,
                totalRawDifferences = run.TotalDifferences,
                missingInBucketB = run.MissingInBucketBCount,
                missingInBucketA = run.MissingInBucketACount,
                sizeMismatches = run.SizeMismatchCount,
                eTagMismatches = run.ETagMismatchCount,
                contentMismatches = run.ContentMismatchCount,
                sequenceGaps = run.SequenceGapCount,
                durationSeconds = run.DurationSeconds
            },
            bucketA = new { name = job.BucketA.BucketName, endpoint = job.BucketA.EndpointUrl, region = job.BucketA.Region },
            bucketB = new { name = job.BucketB.BucketName, endpoint = job.BucketB.EndpointUrl, region = job.BucketB.Region }
        };

        await PostJsonAsync(cfg.AzureWebhookUrl.Trim(), payload, cancellationToken).ConfigureAwait(false);
    }

    private async Task TrackAppInsightsTelemetryAsync(ComparisonJob job, ScanRun run, bool isFailure, CancellationToken cancellationToken)
    {
        var cfg = job.AlertConfig;
        // Parse InstrumentationKey from ConnectionString (e.g. InstrumentationKey=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx;...)
        string key = ExtractAppInsightsKey(cfg.AzureAppInsightsConnectionString);
        if (string.IsNullOrWhiteSpace(key)) return;

        string endpoint = ExtractAppInsightsEndpoint(cfg.AzureAppInsightsConnectionString) ?? "https://dc.services.visualstudio.com/v2/track";

        var metricItem = new
        {
            name = "Microsoft.ApplicationInsights.Metric",
            time = DateTime.UtcNow.ToString("O"),
            iKey = key,
            data = new
            {
                baseType = "MetricData",
                baseData = new
                {
                    ver = 2,
                    metrics = new[]
                    {
                        new { name = "S3Compare.ConfirmedDisparities", value = (double)run.FilteredDifferencesCount },
                        new { name = "S3Compare.TotalDifferences", value = (double)run.TotalDifferences },
                        new { name = "S3Compare.ObjectsEvaluated", value = (double)run.ObjectsCompared }
                    },
                    properties = new Dictionary<string, string>
                    {
                        { "JobName", job.Name },
                        { "JobId", job.Id },
                        { "RunId", run.Id },
                        { "Status", run.Status.ToString() }
                    }
                }
            }
        };

        await PostJsonAsync(endpoint, metricItem, cancellationToken).ConfigureAwait(false);
    }

    public async Task<(bool Success, string Message)> SendTestEmailAsync(JobAlertConfig config, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(config.SmtpHost))
            return (false, "SMTP Host is required.");

        if (string.IsNullOrWhiteSpace(config.ToEmails))
            return (false, "Target recipient email address is required.");

        try
        {
            using var client = new SmtpClient(config.SmtpHost.Trim(), config.SmtpPort)
            {
                EnableSsl = config.SmtpUseSsl,
                DeliveryMethod = SmtpDeliveryMethod.Network,
                Timeout = 12000
            };

            if (!string.IsNullOrWhiteSpace(config.SmtpUsername))
            {
                client.Credentials = new NetworkCredential(config.SmtpUsername.Trim(), config.SmtpPassword);
            }

            string from = string.IsNullOrWhiteSpace(config.FromEmail) ? "s3compareservice@localhost" : config.FromEmail.Trim();
            using var message = new MailMessage
            {
                From = new MailAddress(from, "S3 Compare Service"),
                Subject = $"{config.EmailSubjectPrefix} Test Notification from S3CompareService",
                IsBodyHtml = true,
                Body = "<div style='font-family: sans-serif; padding: 20px;'><h2 style='color: #0E639C;'>S3CompareService - Test Notification</h2><p>This is a test notification confirming that your SMTP settings are configured successfully.</p><p>Timestamp: " + DateTime.UtcNow.ToString("O") + " UTC</p></div>"
            };

            foreach (var r in config.ToEmails.Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries))
            {
                message.To.Add(r.Trim());
            }

            using var smtpCts = CreateDeliveryCts(cancellationToken);
            await client.SendMailAsync(message, smtpCts.Token).ConfigureAwait(false);
            return (true, $"Test email sent successfully to {config.ToEmails} via {config.SmtpHost}:{config.SmtpPort}.");
        }
        catch (Exception ex)
        {
            return (false, $"SMTP Connection / Send Error: {ex.Message}");
        }
    }

    public async Task<(bool Success, string Message)> SendTestAzureWebhookAsync(JobAlertConfig config, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(config.AzureWebhookUrl))
            return (false, "Azure Webhook URL is required.");

        try
        {
            var testPayload = new
            {
                alertType = "TestNotification",
                message = "This is a test alert from S3CompareService verifying Azure Webhook connectivity.",
                timestampUtc = DateTime.UtcNow.ToString("O")
            };

            using var testCts = CreateDeliveryCts(cancellationToken);
            using var resp = await HttpClient.PostAsJsonAsync(config.AzureWebhookUrl.Trim(), testPayload, testCts.Token).ConfigureAwait(false);
            if (resp.IsSuccessStatusCode)
            {
                return (true, $"Azure Webhook responded with HTTP {(int)resp.StatusCode} {resp.StatusCode}. Connection verified!");
            }
            return (false, $"Azure Webhook returned HTTP {(int)resp.StatusCode} {resp.StatusCode}.");
        }
        catch (Exception ex)
        {
            return (false, $"Webhook request failed: {ex.Message}");
        }
    }

    public Task<(bool Success, string Message)> TestSmtpConnectionAsync(JobAlertConfig config, CancellationToken cancellationToken = default)
        => SendTestEmailAsync(config, cancellationToken);

    public Task<(bool Success, string Message)> TestAzureWebhookAsync(JobAlertConfig config, CancellationToken cancellationToken = default)
        => SendTestAzureWebhookAsync(config, cancellationToken);

    public Task<(bool ShouldSend, string Reason)> EvaluateDailyDigestConditionAsync(ComparisonJob job, DateTime nowUtc)
    {
        var cfg = job.AlertConfig;
        if (!cfg.IsEnabled || !cfg.SendDailyDigest)
        {
            return Task.FromResult((false, "Daily status digest is disabled for this job."));
        }

        // 1. Has it already been sent today?
        if (cfg.LastDailyDigestSentUtc.HasValue && cfg.LastDailyDigestSentUtc.Value.Date == nowUtc.Date)
        {
            return Task.FromResult((false, $"Daily digest already sent today at {cfg.LastDailyDigestSentUtc.Value:HH:mm} UTC."));
        }

        // 2. Has the scheduled hour been reached?
        if (nowUtc.Hour < cfg.DailyDigestHourUtc)
        {
            return Task.FromResult((false, $"Current hour ({nowUtc.Hour:00}:00 UTC) has not reached scheduled digest hour ({cfg.DailyDigestHourUtc:00}:00 UTC)."));
        }

        // 3. If OnlyIfNoRecentAlerts is true, verify no alert was sent in the last 24 hours
        if (cfg.OnlyIfNoRecentAlerts && cfg.LastAlertSentUtc.HasValue)
        {
            var hoursSinceAlert = (nowUtc - cfg.LastAlertSentUtc.Value).TotalHours;
            if (hoursSinceAlert < 24.0)
            {
                return Task.FromResult((false, $"Suppressed: An alert was triggered {hoursSinceAlert:F1} hours ago (less than 24h)."));
            }
        }

        return Task.FromResult((true, "Daily status report is due."));
    }

    /// <summary>Hours after which the latest scan is considered too old for an "all clear".</summary>
    public const double DigestMaxScanAgeHours = 26.0;

    public sealed record DigestAssessment(bool IsAllClear, string Headline, IReadOnlyList<string> Issues);

    /// <summary>
    /// Decides whether the daily digest may say "all clear": the latest run must be recent (within ~26h),
    /// Completed, and have zero confirmed disparities, with no failed runs in the last 24h. Otherwise the
    /// digest is still sent but describes the actual problem.
    /// </summary>
    public static DigestAssessment AssessDigest(ScanRun? latestRun, IReadOnlyCollection<ScanRun> recentRuns24h, DateTime nowUtc)
    {
        var issues = new List<string>();

        if (latestRun == null)
        {
            issues.Add("No scan has been recorded for this job.");
        }
        else
        {
            var reference = latestRun.EndTimeUtc ?? latestRun.StartTimeUtc;
            double ageHours = (nowUtc - reference).TotalHours;
            if (ageHours > DigestMaxScanAgeHours)
            {
                issues.Add($"The latest scan finished {ageHours:F1} hours ago (more than {DigestMaxScanAgeHours:F0}h) - scans may have stopped running.");
            }
            if (latestRun.Status != RunStatus.Completed)
            {
                issues.Add($"The latest scan did not complete successfully (status: {latestRun.Status}).");
            }
            else if (latestRun.FilteredDifferencesCount > 0)
            {
                issues.Add($"The latest scan found {latestRun.FilteredDifferencesCount:N0} confirmed disparit{(latestRun.FilteredDifferencesCount == 1 ? "y" : "ies")}.");
            }
        }

        int failed = recentRuns24h.Count(r => r.Status == RunStatus.Failed);
        if (failed > 0)
        {
            issues.Add($"{failed} scan(s) failed in the past 24 hours.");
        }

        return issues.Count == 0
            ? new DigestAssessment(true, "All systems operational", issues)
            : new DigestAssessment(false, "Attention needed", issues);
    }

    /// <summary>
    /// Sends the daily digest. Returns true when at least one channel delivered; only then is
    /// <c>AlertConfig.LastDailyDigestSentUtc</c> set on the passed job (persist it with
    /// <c>IJobRepository.UpdateAlertTimestampsAsync</c>).
    /// </summary>
    public async Task<bool> DispatchDailyDigestAsync(ComparisonJob job, ScanRun? latestRun, List<ScanRun> recentRuns24h, CancellationToken cancellationToken = default)
    {
        var result = await DispatchDailyDigestCoreAsync(job, latestRun, recentRuns24h, cancellationToken).ConfigureAwait(false);
        return result.Success;
    }

    private async Task<(bool Success, List<string> Errors)> DispatchDailyDigestCoreAsync(ComparisonJob job, ScanRun? latestRun, List<ScanRun> recentRuns24h, CancellationToken cancellationToken)
    {
        var cfg = job.AlertConfig;
        var errors = new List<string>();
        if (!cfg.IsEnabled || !cfg.SendDailyDigest)
        {
            errors.Add("Daily digest is disabled for this job.");
            return (false, errors);
        }

        int totalRuns24h = recentRuns24h.Count;
        int completedRuns24h = recentRuns24h.Count(r => r.Status == RunStatus.Completed);
        int failedRuns24h = recentRuns24h.Count(r => r.Status == RunStatus.Failed);
        var assessment = AssessDigest(latestRun, recentRuns24h, DateTime.UtcNow);
        bool anyDelivered = false;
        string label = assessment.IsAllClear ? "all-clear" : "attention-needed";

        // 1. Send Email via SMTP
        if (!string.IsNullOrWhiteSpace(cfg.SmtpHost) && !string.IsNullOrWhiteSpace(cfg.ToEmails))
        {
            try
            {
                using var cts = CreateDeliveryCts(cancellationToken);
                await SendDailyDigestEmailAsync(job, latestRun, totalRuns24h, completedRuns24h, failedRuns24h, assessment, cts.Token).ConfigureAwait(false);
                anyDelivered = true;
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("INFO", "Alerts", $"Daily {label} digest sent to {cfg.ToEmails} for job '{job.Name}'.", scanRunId: latestRun?.Id, jobId: job.Id, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                errors.Add($"Email: {ex.Message}");
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("ERROR", "Alerts", $"Failed to send daily digest email to {cfg.ToEmails}: {ex.Message}", details: ex.StackTrace, scanRunId: latestRun?.Id, jobId: job.Id, cancellationToken: CancellationToken.None);
                }
            }
        }

        // 2. Dispatch Azure Webhook if configured
        if (!string.IsNullOrWhiteSpace(cfg.AzureWebhookUrl))
        {
            try
            {
                var payload = new
                {
                    alertType = assessment.IsAllClear ? "DailyDigestAllClear" : "DailyDigestAttentionNeeded",
                    jobId = job.Id,
                    jobName = job.Name,
                    isCritical = !assessment.IsAllClear,
                    status = assessment.IsAllClear ? "AllClear" : "AttentionNeeded",
                    issues = assessment.Issues,
                    message = assessment.IsAllClear
                        ? $"Daily all-clear status confirmation for '{job.Name}'. S3CompareService is actively running and healthy."
                        : $"Daily status for '{job.Name}': {string.Join(" ", assessment.Issues)}",
                    timestampUtc = DateTime.UtcNow.ToString("O"),
                    metrics = new
                    {
                        scansCompleted24h = completedRuns24h,
                        failedScans24h = failedRuns24h,
                        totalScans24h = totalRuns24h,
                        objectsCompared = latestRun?.ObjectsCompared ?? 0,
                        matchedCount = latestRun?.MatchedCount ?? 0,
                        confirmedDisparities = latestRun?.FilteredDifferencesCount ?? 0,
                        inFlightReplicationLag = latestRun?.InFlightDifferencesCount ?? 0,
                        lastScanTimeUtc = latestRun?.EndTimeUtc?.ToString("O")
                    }
                };

                await PostJsonAsync(cfg.AzureWebhookUrl.Trim(), payload, cancellationToken).ConfigureAwait(false);
                anyDelivered = true;
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("INFO", "Alerts", $"Daily {label} webhook dispatched to Azure for job '{job.Name}'.", scanRunId: latestRun?.Id, jobId: job.Id, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                errors.Add($"Webhook: {ex.Message}");
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("ERROR", "Alerts", $"Failed to dispatch daily digest to Azure Webhook: {ex.Message}", details: ex.StackTrace, scanRunId: latestRun?.Id, jobId: job.Id, cancellationToken: CancellationToken.None);
                }
            }
        }

        if (!anyDelivered && errors.Count == 0)
        {
            errors.Add("No delivery channel (SMTP recipients or webhook URL) is configured.");
        }

        if (anyDelivered)
        {
            cfg.LastDailyDigestSentUtc = DateTime.UtcNow;
        }

        return (anyDelivered, errors);
    }

    private async Task SendDailyDigestEmailAsync(ComparisonJob job, ScanRun? latestRun, int totalRuns24h, int completedRuns24h, int failedRuns24h, DigestAssessment assessment, CancellationToken cancellationToken)
    {
        var cfg = job.AlertConfig;
        using var client = new SmtpClient(cfg.SmtpHost.Trim(), cfg.SmtpPort)
        {
            EnableSsl = cfg.SmtpUseSsl,
            DeliveryMethod = SmtpDeliveryMethod.Network,
            Timeout = 15000
        };

        if (!string.IsNullOrWhiteSpace(cfg.SmtpUsername))
        {
            client.Credentials = new NetworkCredential(cfg.SmtpUsername.Trim(), cfg.SmtpPassword);
        }

        string from = string.IsNullOrWhiteSpace(cfg.FromEmail) ? "s3compareservice@localhost" : cfg.FromEmail.Trim();
        string subject = assessment.IsAllClear
            ? $"{cfg.EmailSubjectPrefix} [DAILY ALL-CLEAR] - Job '{job.Name}' - All Systems Operational"
            : $"{cfg.EmailSubjectPrefix} [DAILY STATUS - ATTENTION] - Job '{job.Name}'";

        using var message = new MailMessage
        {
            From = new MailAddress(from, "S3 Compare Service"),
            Subject = subject,
            IsBodyHtml = true,
            Body = BuildDailyDigestHtmlEmailBody(job, latestRun, totalRuns24h, completedRuns24h, failedRuns24h, assessment)
        };

        var recipients = cfg.ToEmails.Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var r in recipients)
        {
            message.To.Add(r.Trim());
        }

        await client.SendMailAsync(message, cancellationToken).ConfigureAwait(false);
    }

    private static string BuildDailyDigestHtmlEmailBody(ComparisonJob job, ScanRun? latestRun, int totalRuns24h, int completedRuns24h, int failedRuns24h, DigestAssessment assessment)
    {
        long objects = latestRun?.ObjectsCompared ?? 0;
        long matched = latestRun?.MatchedCount ?? 0;
        long disparities = latestRun?.FilteredDifferencesCount ?? 0;
        long inFlight = latestRun?.InFlightDifferencesCount ?? 0;
        double matchRate = objects > 0 ? (matched * 100.0 / objects) : 100.0;

        bool ok = assessment.IsAllClear;
        string headerBg = ok ? "#107C41" : "#B26A00";
        string headerTitle = ok ? "✅ DAILY ALL-CLEAR STATUS CONFIRMATION" : "⚠️ DAILY STATUS: ATTENTION NEEDED";
        string boxBg = ok ? "#e6f4ea" : "#fff3cd";
        string boxBorder = ok ? "#137333" : "#ffc107";
        string boxTitle = ok ? "🛡️ Service Operational &amp; Healthy" : "Attention needed";
        string boxBody = ok
            ? "This is your daily confirmation that <strong>S3CompareService</strong> is actively running and continuously monitoring bucket synchronization. The latest scan completed successfully with no confirmed disparities, and no scans failed in the past 24 hours."
            : "<ul style='margin: 6px 0 0 18px; padding: 0;'>" + string.Concat(assessment.Issues.Select(i => $"<li>{WebUtility.HtmlEncode(i)}</li>")) + "</ul>";
        string badgeText = ok ? "OPERATIONAL - ALL CLEAR" : "ATTENTION NEEDED";
        string badgeBg = ok ? "#52c41a" : "#faad14";
        string disparityColor = disparities > 0 ? "#dc3545" : "#28a745";
        string disparityNote = disparities > 0 ? "(see latest scan)" : "(All buckets in sync)";
        string latestScanTime = latestRun != null ? $"{latestRun.StartTimeUtc:yyyy-MM-dd HH:mm:ss} UTC" : "N/A";

        return $@"<!DOCTYPE html>
<html>
<head>
<style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f4f6f9; color: #333333; margin: 0; padding: 20px; }}
    .container {{ max-width: 650px; margin: 0 auto; background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.1); border: 1px solid #e1e4e8; }}
    .header {{ background-color: {headerBg}; color: #ffffff; padding: 20px 24px; }}
    .header h1 {{ margin: 0; font-size: 18px; letter-spacing: 0.5px; }}
    .content {{ padding: 24px; }}
    .success-box {{ background-color: {boxBg}; border-left: 4px solid {boxBorder}; padding: 14px 18px; margin-bottom: 20px; border-radius: 4px; }}
    .table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
    .table th, .table td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #e9ecef; font-size: 13px; }}
    .table th {{ background-color: #f8f9fa; font-weight: 600; color: #495057; }}
    .badge {{ display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; background: {badgeBg}; color: #ffffff; }}
    .footer {{ background: #f8f9fa; padding: 16px 24px; font-size: 11px; color: #6c757d; border-top: 1px solid #e9ecef; text-align: center; }}
</style>
</head>
<body>
<div class='container'>
    <div class='header'>
        <h1>{headerTitle}</h1>
        <div style='font-size: 13px; opacity: 0.9; margin-top: 4px;'>Job: <strong>{WebUtility.HtmlEncode(job.Name)}</strong> | Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC</div>
    </div>
    <div class='content'>
        <div class='success-box'>
            <strong style='font-size: 14px;'>{boxTitle}</strong>
            <p style='margin: 6px 0 0 0; font-size: 13px; color: #202124;'>
                {boxBody}
            </p>
        </div>

        <table class='table'>
            <tr><th colspan='2'>24-Hour Activity &amp; Health Metrics</th></tr>
            <tr><td><strong>System Health Status</strong></td><td><span class='badge'>{badgeText}</span></td></tr>
            <tr><td><strong>Scans Completed (Past 24h)</strong></td><td><strong>{completedRuns24h}</strong> successful (Failed: {failedRuns24h}, Total: {totalRuns24h})</td></tr>
            <tr><td><strong>Confirmed Disparities (latest scan)</strong></td><td><strong style='color: {disparityColor};'>{disparities:N0}</strong> {disparityNote}</td></tr>
            <tr><td><strong>In-Flight Replication Lag</strong></td><td>{inFlight:N0} <span style='font-size: 11px; color: #888888;'>(shielded by {job.ReplicationGracePeriodMinutes}m grace window)</span></td></tr>
            <tr><td><strong>Objects Evaluated</strong></td><td>{objects:N0}</td></tr>
            <tr><td><strong>Data Match Rate</strong></td><td>{matchRate:F2}%</td></tr>
            <tr><td><strong>Latest Scan Time</strong></td><td>{latestScanTime}</td></tr>
            <tr><td><strong>Scan Duration</strong></td><td>{latestRun?.FormattedDuration ?? "N/A"}</td></tr>
        </table>

        <table class='table'>
            <tr><th colspan='2'>Monitored Buckets</th></tr>
            <tr><td><strong>Bucket A (Source)</strong></td><td>{WebUtility.HtmlEncode(job.BucketA.BucketName)} ({WebUtility.HtmlEncode(job.BucketA.Region)})</td></tr>
            <tr><td><strong>Bucket B (Target)</strong></td><td>{WebUtility.HtmlEncode(job.BucketB.BucketName)} ({WebUtility.HtmlEncode(job.BucketB.Region)})</td></tr>
        </table>
    </div>
    <div class='footer'>
        Daily digest delivered by <strong>S3CompareService</strong>. Next report scheduled in 24 hours.
    </div>
</div>
</body>
</html>";
    }

    public async Task<(bool Success, string Message)> SendTestDailyDigestAsync(JobAlertConfig config, ComparisonJob job, ScanRun? sampleRun = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(config.SmtpHost) && string.IsNullOrWhiteSpace(config.AzureWebhookUrl))
        {
            return (false, "Please configure either SMTP settings or an Azure Webhook URL to test the daily digest.");
        }

        var run = sampleRun ?? new ScanRun
        {
            JobId = job.Id,
            JobName = job.Name,
            StartTimeUtc = DateTime.UtcNow.AddMinutes(-5),
            EndTimeUtc = DateTime.UtcNow,
            DurationSeconds = 300,
            Status = RunStatus.Completed,
            ObjectsCompared = 75000,
            MatchedCount = 75000,
            FilteredDifferencesCount = 0,
            InFlightDifferencesCount = 3,
            TotalDifferences = 3
        };

        var recentRuns = new List<ScanRun> { run };

        // Work on a copy so a test never touches the caller's LastDailyDigestSentUtc, and force the digest on.
        var configCopy = SecretMask.Clone(config);
        configCopy.IsEnabled = true;
        configCopy.SendDailyDigest = true;
        var tempJob = new ComparisonJob
        {
            Id = job.Id,
            Name = job.Name,
            AlertConfig = configCopy,
            BucketA = job.BucketA,
            BucketB = job.BucketB,
            ReplicationGracePeriodMinutes = job.ReplicationGracePeriodMinutes
        };

        var (success, errors) = await DispatchDailyDigestCoreAsync(tempJob, run, recentRuns, cancellationToken).ConfigureAwait(false);
        return success
            ? (true, errors.Count == 0
                ? "Test daily digest dispatched successfully!"
                : $"Test daily digest delivered on at least one channel, but some failed: {string.Join("; ", errors)}")
            : (false, $"Test daily digest could not be delivered: {string.Join("; ", errors)}");
    }

    private static string ExtractAppInsightsKey(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return string.Empty;
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
        foreach (var p in parts)
        {
            var kv = p.Split('=', 2);
            if (kv.Length == 2 && kv[0].Trim().Equals("InstrumentationKey", StringComparison.OrdinalIgnoreCase))
            {
                return kv[1].Trim();
            }
        }
        return string.Empty;
    }

    private static string? ExtractAppInsightsEndpoint(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return null;
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
        foreach (var p in parts)
        {
            var kv = p.Split('=', 2);
            if (kv.Length == 2 && kv[0].Trim().Equals("IngestionEndpoint", StringComparison.OrdinalIgnoreCase))
            {
                string baseUri = kv[1].Trim().TrimEnd('/');
                return $"{baseUri}/v2/track";
            }
        }
        return null;
    }
}
