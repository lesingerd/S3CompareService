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
    Task DispatchAlertsAsync(ComparisonJob job, ScanRun run, List<string> reasons, CancellationToken cancellationToken = default);
    Task<(bool Success, string Message)> SendTestEmailAsync(JobAlertConfig config, CancellationToken cancellationToken = default);
    Task<(bool Success, string Message)> SendTestAzureWebhookAsync(JobAlertConfig config, CancellationToken cancellationToken = default);
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
        if (cfg.AlertOnDisparities)
        {
            if (run.FilteredDifferencesCount > cfg.MaxDisparitiesThreshold)
            {
                reasons.Add($"Confirmed Disparities: {run.FilteredDifferencesCount:N0} (Exceeds threshold of {cfg.MaxDisparitiesThreshold:N0})");
            }
            if (run.MissingInBucketBCount > cfg.MaxMissingInBThreshold)
            {
                reasons.Add($"Missing in Bucket B: {run.MissingInBucketBCount:N0} (Exceeds threshold of {cfg.MaxMissingInBThreshold:N0})");
            }
            if (run.MissingInBucketACount > cfg.MaxMissingInAThreshold)
            {
                reasons.Add($"Missing in Bucket A: {run.MissingInBucketACount:N0} (Exceeds threshold of {cfg.MaxMissingInAThreshold:N0})");
            }
            if (run.SizeMismatchCount > cfg.MaxSizeMismatchThreshold)
            {
                reasons.Add($"Size Mismatches: {run.SizeMismatchCount:N0} (Exceeds threshold of {cfg.MaxSizeMismatchThreshold:N0})");
            }
            if (run.ETagMismatchCount > cfg.MaxETagMismatchThreshold)
            {
                reasons.Add($"ETag Mismatches: {run.ETagMismatchCount:N0} (Exceeds threshold of {cfg.MaxETagMismatchThreshold:N0})");
            }
            if (run.ContentMismatchCount > cfg.MaxContentMismatchThreshold)
            {
                reasons.Add($"Content Byte Mismatches: {run.ContentMismatchCount:N0} (Exceeds threshold of {cfg.MaxContentMismatchThreshold:N0})");
            }
            if (run.SequenceGapCount > cfg.MaxSequenceGapThreshold)
            {
                reasons.Add($"Sequence Gaps: {run.SequenceGapCount:N0} (Exceeds threshold of {cfg.MaxSequenceGapThreshold:N0})");
            }
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

    public async Task DispatchAlertsAsync(ComparisonJob job, ScanRun run, List<string> reasons, CancellationToken cancellationToken = default)
    {
        var cfg = job.AlertConfig;
        if (!cfg.IsEnabled) return;

        bool isFailure = run.Status == RunStatus.Failed || (reasons.Count > 0 && !reasons[0].StartsWith("Periodic status"));

        // 1. Dispatch Email via SMTP
        if (!string.IsNullOrWhiteSpace(cfg.SmtpHost) && !string.IsNullOrWhiteSpace(cfg.ToEmails))
        {
            try
            {
                await SendEmailAlertAsync(job, run, reasons, isFailure, cancellationToken).ConfigureAwait(false);
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("INFO", "Alerts", $"SMTP alert sent successfully to {cfg.ToEmails} for job '{job.Name}'.", scanRunId: run.Id, jobId: job.Id, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("ERROR", "Alerts", $"Failed to send SMTP alert to {cfg.ToEmails}: {ex.Message}", details: ex.StackTrace, scanRunId: run.Id, jobId: job.Id, cancellationToken: cancellationToken);
                }
            }
        }

        // 2. Dispatch Azure Webhook (Logic Apps / Action Groups / Azure Functions)
        if (!string.IsNullOrWhiteSpace(cfg.AzureWebhookUrl))
        {
            try
            {
                await SendAzureWebhookAlertAsync(job, run, reasons, isFailure, cancellationToken).ConfigureAwait(false);
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("INFO", "Alerts", $"Azure Webhook alert dispatched successfully for job '{job.Name}'.", scanRunId: run.Id, jobId: job.Id, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                if (_auditRepo != null)
                {
                    await _auditRepo.LogAsync("ERROR", "Alerts", $"Failed to dispatch Azure Webhook alert: {ex.Message}", details: ex.StackTrace, scanRunId: run.Id, jobId: job.Id, cancellationToken: cancellationToken);
                }
            }
        }

        // 3. Dispatch to Azure Application Insights Telemetry
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
                    await _auditRepo.LogAsync("WARN", "Alerts", $"Failed to post App Insights telemetry: {ex.Message}", scanRunId: run.Id, jobId: job.Id, cancellationToken: cancellationToken);
                }
            }
        }
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

        var resp = await HttpClient.PostAsJsonAsync(cfg.AzureWebhookUrl.Trim(), payload, cancellationToken).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
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

        await HttpClient.PostAsJsonAsync(endpoint, metricItem, cancellationToken).ConfigureAwait(false);
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

            await client.SendMailAsync(message, cancellationToken).ConfigureAwait(false);
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

            var resp = await HttpClient.PostAsJsonAsync(config.AzureWebhookUrl.Trim(), testPayload, cancellationToken).ConfigureAwait(false);
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
