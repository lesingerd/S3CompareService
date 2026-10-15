using System.IO;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.UI.Services;

public class ServiceStatusDto
{
    public string Service { get; set; } = string.Empty;
    public string Version { get; set; } = string.Empty;
    public string Status { get; set; } = "Offline";
    public string Uptime { get; set; } = "-";
    public double MemoryMB { get; set; }
    public string DatabasePath { get; set; } = string.Empty;
    public int ActiveScanCount { get; set; }
    public List<ActiveScanDto> ActiveScans { get; set; } = new();
}

public class ActiveScanDto
{
    public string ScanRunId { get; set; } = string.Empty;
    public string JobId { get; set; } = string.Empty;
    public string JobName { get; set; } = string.Empty;
    public DateTime StartTimeUtc { get; set; }
    public string Duration { get; set; } = string.Empty;
    public long ObjectsCompared { get; set; }
    public long MatchedCount { get; set; }
    public long TotalDifferences { get; set; }
    public long FilteredDifferencesCount { get; set; }
    public long InFlightDifferencesCount { get; set; }
    public double ItemsPerSecond { get; set; }
    public string CurrentKeyA { get; set; } = "-";
    public string CurrentKeyB { get; set; } = "-";
}

public class DifferencePageResult
{
    public int TotalCount { get; set; }
    public int Skip { get; set; }
    public int Take { get; set; }
    public List<RecordedDifference> Items { get; set; } = new();
}

public class ServiceClient
{
    private readonly HttpClient _http;
    private readonly string _baseUrl;
    private readonly SqliteConnectionFactory _directDbFactory;
    private readonly JobRepository _directJobRepo;
    private readonly ScanRunRepository _directScanRunRepo;
    private readonly DifferenceRepository _directDiffRepo;
    private readonly AuditLogRepository _directAuditRepo;

    public ServiceClient(string baseUrl = "http://localhost:5088")
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _http = new HttpClient { BaseAddress = new Uri(_baseUrl), Timeout = TimeSpan.FromSeconds(10) };

        _directDbFactory = new SqliteConnectionFactory();
        _directJobRepo = new JobRepository(_directDbFactory);
        _directScanRunRepo = new ScanRunRepository(_directDbFactory);
        _directDiffRepo = new DifferenceRepository(_directDbFactory);
        _directAuditRepo = new AuditLogRepository(_directDbFactory);
    }

    public async Task<ServiceStatusDto> GetStatusAsync()
    {
        try
        {
            var result = await _http.GetFromJsonAsync<ServiceStatusDto>("/api/status");
            if (result != null) return result;
        }
        catch
        {
            // Fallback when service is not running
        }

        return new ServiceStatusDto
        {
            Service = "S3CompareService",
            Status = "Stopped / Offline",
            DatabasePath = DatabaseConfig.GetDefaultDatabasePath()
        };
    }

    public async Task<List<S3EndpointPreset>> GetPresetsAsync()
    {
        try
        {
            var res = await _http.GetFromJsonAsync<List<S3EndpointPreset>>("/api/presets");
            if (res != null) return res;
        }
        catch
        {
        }
        return S3CompareService.Core.Engine.S3ClientFactory.GetWasabiEndpoints();
    }

    public async Task<(bool Success, string Message)> TestConnectionAsync(S3BucketConfig config)
    {
        try
        {
            var resp = await _http.PostAsJsonAsync("/api/test-connection", config);
            if (resp.IsSuccessStatusCode)
            {
                var doc = await resp.Content.ReadFromJsonAsync<JsonElement>();
                bool ok = doc.GetProperty("success").GetBoolean();
                string msg = doc.GetProperty("message").GetString() ?? "";
                return (ok, msg);
            }
        }
        catch
        {
        }
        // Direct test fallback
        return await S3CompareService.Core.Engine.S3ClientFactory.TestConnectionAsync(config);
    }

    public async Task<List<ComparisonJob>> GetJobsAsync()
    {
        try
        {
            var res = await _http.GetFromJsonAsync<List<ComparisonJob>>("/api/jobs");
            if (res != null) return res;
        }
        catch
        {
        }
        // Direct DB fallback
        return await _directJobRepo.GetAllAsync();
    }

    public async Task SaveJobAsync(ComparisonJob job)
    {
        bool apiSuccess = false;
        try
        {
            var resp = await _http.PutAsJsonAsync($"/api/jobs/{job.Id}", job);
            if (resp.IsSuccessStatusCode) apiSuccess = true;
        }
        catch
        {
        }

        if (!apiSuccess)
        {
            await _directJobRepo.SaveAsync(job);
        }
    }

    public async Task DeleteJobAsync(string id)
    {
        try
        {
            await _http.DeleteAsync($"/api/jobs/{id}");
        }
        catch
        {
            await _directJobRepo.DeleteAsync(id);
        }
    }

    public async Task<(bool Success, string Message)> TriggerJobAsync(string jobId)
    {
        try
        {
            var resp = await _http.PostAsync($"/api/jobs/{jobId}/run", null);
            var doc = await resp.Content.ReadFromJsonAsync<JsonElement>();
            string msg = doc.GetProperty("message").GetString() ?? "";
            return (resp.IsSuccessStatusCode, msg);
        }
        catch (Exception ex)
        {
            return (false, $"Could not communicate with background service: {ex.Message}. Make sure the service is running.");
        }
    }

    public async Task<List<ScanRun>> GetRunsAsync(string? jobId = null, int count = 50)
    {
        try
        {
            string url = string.IsNullOrWhiteSpace(jobId) ? $"/api/runs?count={count}" : $"/api/runs?jobId={jobId}&count={count}";
            var res = await _http.GetFromJsonAsync<List<ScanRun>>(url);
            if (res != null) return res;
        }
        catch
        {
        }
        return await _directScanRunRepo.GetRecentRunsAsync(count, jobId);
    }

    public async Task<List<BucketMetadataSnapshot>> GetSnapshotsAsync(string runId)
    {
        try
        {
            var res = await _http.GetFromJsonAsync<List<BucketMetadataSnapshot>>($"/api/runs/{runId}/snapshots");
            if (res != null) return res;
        }
        catch
        {
        }
        return await _directScanRunRepo.GetMetadataSnapshotsAsync(runId);
    }

    public async Task<DifferencePageResult> GetDifferencesAsync(
        string runId,
        DifferenceType? type = null,
        bool? withinGracePeriod = null,
        string? searchKey = null,
        int skip = 0,
        int take = 50)
    {
        try
        {
            string url = $"/api/runs/{runId}/differences?skip={skip}&take={take}";
            if (type.HasValue) url += $"&type={type.Value}";
            if (withinGracePeriod.HasValue) url += $"&withinGracePeriod={withinGracePeriod.Value}";
            if (!string.IsNullOrWhiteSpace(searchKey)) url += $"&searchKey={Uri.EscapeDataString(searchKey)}";

            var res = await _http.GetFromJsonAsync<DifferencePageResult>(url);
            if (res != null) return res;
        }
        catch
        {
        }

        // Direct DB fallback
        var items = await _directDiffRepo.GetDifferencesAsync(runId, type, withinGracePeriod, searchKey, skip, take);
        int total = await _directDiffRepo.GetDifferenceCountAsync(runId, type, withinGracePeriod, searchKey);
        return new DifferencePageResult { TotalCount = total, Skip = skip, Take = take, Items = items };
    }

    public async Task<List<ServiceAuditLog>> GetLogsAsync(int count = 100)
    {
        try
        {
            var res = await _http.GetFromJsonAsync<List<ServiceAuditLog>>($"/api/logs?count={count}");
            if (res != null) return res;
        }
        catch
        {
        }
        return await _directAuditRepo.GetRecentLogsAsync(count);
    }

    public async Task<bool> PauseRunAsync(string runId)
    {
        try
        {
            var resp = await _http.PostAsync($"/api/runs/{runId}/pause", null);
            return resp.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> ResumeRunAsync(string runId)
    {
        try
        {
            var resp = await _http.PostAsync($"/api/runs/{runId}/resume", null);
            return resp.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> CancelRunAsync(string runId)
    {
        try
        {
            var resp = await _http.PostAsync($"/api/runs/{runId}/cancel", null);
            return resp.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<(bool Success, string Message)> TestSmtpAsync(JobAlertConfig config)
    {
        try
        {
            var resp = await _http.PostAsJsonAsync("/api/alerts/test-smtp", config);
            if (resp.IsSuccessStatusCode)
            {
                var result = await resp.Content.ReadFromJsonAsync<AlertTestResponse>();
                if (result != null) return (result.Success, result.Message);
            }
        }
        catch
        {
            // Worker service may be offline - fallback to direct test
        }

        var directService = new S3CompareService.Core.Notifications.AlertNotificationService();
        return await directService.TestSmtpConnectionAsync(config);
    }

    public async Task<(bool Success, string Message)> TestAzureWebhookAsync(JobAlertConfig config)
    {
        try
        {
            var resp = await _http.PostAsJsonAsync("/api/alerts/test-azure-webhook", config);
            if (resp.IsSuccessStatusCode)
            {
                var result = await resp.Content.ReadFromJsonAsync<AlertTestResponse>();
                if (result != null) return (result.Success, result.Message);
            }
        }
        catch
        {
            // Worker service may be offline - fallback to direct test
        }

        var directService = new S3CompareService.Core.Notifications.AlertNotificationService();
        return await directService.TestAzureWebhookAsync(config);
    }

    public async Task<(bool Success, string Message)> TestDailyDigestAsync(JobAlertConfig config, ComparisonJob job)
    {
        try
        {
            var resp = await _http.PostAsJsonAsync("/api/alerts/test-daily-digest", config);
            if (resp.IsSuccessStatusCode)
            {
                var result = await resp.Content.ReadFromJsonAsync<AlertTestResponse>();
                if (result != null) return (result.Success, result.Message);
            }
        }
        catch
        {
            // Worker service may be offline - fallback to direct test
        }

        var directService = new S3CompareService.Core.Notifications.AlertNotificationService();
        return await directService.SendTestDailyDigestAsync(config, job);
    }
}

public class AlertTestResponse
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
}
