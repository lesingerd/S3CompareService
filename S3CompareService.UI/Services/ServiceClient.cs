using System.IO;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Sockets;
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
    public bool IsPaused { get; set; }
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

/// <summary>Thrown when the service could not be reached at all (connection refused).</summary>
public class ServiceOfflineException : Exception
{
    public ServiceOfflineException(string message, Exception? inner = null) : base(message, inner) { }
}

/// <summary>Thrown when the service replied with an error, or the request failed in a way that is not "offline".</summary>
public class ServiceApiException : Exception
{
    public System.Net.HttpStatusCode? StatusCode { get; }
    public ServiceApiException(string message, System.Net.HttpStatusCode? statusCode = null, Exception? inner = null)
        : base(message, inner) { StatusCode = statusCode; }
}

public class ServiceClient
{
    public const string DefaultBaseUrl = "http://localhost:5088";
    public const string SecretMask = "********";
    private const string OfflineWriteMessage = "The S3CompareService background service is not reachable. The service must be running to change jobs or control scans.";

    private readonly HttpClient _http;      // short calls
    private readonly HttpClient _longHttp;  // exports / large downloads
    private readonly SqliteConnectionFactory _directDbFactory;
    private readonly DatabaseMigrator _directMigrator;
    private readonly JobRepository _directJobRepo;
    private readonly ScanRunRepository _directScanRunRepo;
    private readonly DifferenceRepository _directDiffRepo;
    private readonly AuditLogRepository _directAuditRepo;
    private readonly SemaphoreSlim _directDbInitLock = new(1, 1);
    private volatile bool _directDbInitialized;

    public string BaseUrl { get; }

    public ServiceClient(string? baseUrl = null)
    {
        BaseUrl = (string.IsNullOrWhiteSpace(baseUrl) ? ResolveBaseUrl() : baseUrl).TrimEnd('/');

        var handler = new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5) };
        _http = CreateClient(handler, TimeSpan.FromSeconds(15));
        _longHttp = CreateClient(handler, TimeSpan.FromMinutes(30));

        _directDbFactory = new SqliteConnectionFactory();
        _directMigrator = new DatabaseMigrator(_directDbFactory);
        _directJobRepo = new JobRepository(_directDbFactory);
        _directScanRunRepo = new ScanRunRepository(_directDbFactory);
        _directDiffRepo = new DifferenceRepository(_directDbFactory);
        _directAuditRepo = new AuditLogRepository(_directDbFactory);
    }

    private HttpClient CreateClient(HttpMessageHandler handler, TimeSpan timeout)
    {
        var client = new HttpClient(handler, disposeHandler: false) { BaseAddress = new Uri(BaseUrl), Timeout = timeout };
        client.DefaultRequestHeaders.Add("X-S3Compare-Client", "ui");
        return client;
    }

    /// <summary>Order: env S3COMPARE_SERVICE_URL, then "ServiceUrl" in ui-settings.json next to the exe, then the default.</summary>
    public static string ResolveBaseUrl()
    {
        static bool Valid(string? v) =>
            Uri.TryCreate(v, UriKind.Absolute, out var u) && (u.Scheme == Uri.UriSchemeHttp || u.Scheme == Uri.UriSchemeHttps);

        var env = Environment.GetEnvironmentVariable("S3COMPARE_SERVICE_URL");
        if (Valid(env)) return env!.Trim();

        try
        {
            var path = Path.Combine(AppContext.BaseDirectory, "ui-settings.json");
            if (File.Exists(path))
            {
                using var doc = JsonDocument.Parse(File.ReadAllText(path),
                    new JsonDocumentOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip });
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    if (prop.Name.Equals("ServiceUrl", StringComparison.OrdinalIgnoreCase) && prop.Value.ValueKind == JsonValueKind.String)
                    {
                        var v = prop.Value.GetString();
                        if (Valid(v)) return v!.Trim();
                    }
                }
            }
        }
        catch
        {
            // Ignore malformed settings; use the default.
        }
        return DefaultBaseUrl;
    }

    private async Task EnsureDirectDbInitializedAsync()
    {
        if (_directDbInitialized) return;
        await _directDbInitLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_directDbInitialized) return;
            await _directMigrator.MigrateAsync().ConfigureAwait(false);
            _directDbInitialized = true;
        }
        finally
        {
            _directDbInitLock.Release();
        }
    }

    // ---------- HTTP plumbing ----------

    private static bool IsConnectionRefused(Exception ex) =>
        ex is HttpRequestException { InnerException: SocketException { SocketErrorCode: SocketError.ConnectionRefused } };

    private async Task<HttpResponseMessage> SendAsync(HttpClient http, HttpRequestMessage request, HttpCompletionOption option = HttpCompletionOption.ResponseContentRead)
    {
        HttpResponseMessage resp;
        try
        {
            resp = await http.SendAsync(request, option).ConfigureAwait(false);
        }
        catch (HttpRequestException ex) when (IsConnectionRefused(ex))
        {
            throw new ServiceOfflineException($"Could not connect to the service at {BaseUrl} (connection refused).", ex);
        }
        catch (HttpRequestException ex)
        {
            throw new ServiceApiException($"Could not communicate with the service at {BaseUrl}: {ex.Message}", null, ex);
        }
        catch (TaskCanceledException ex)
        {
            throw new ServiceApiException($"The request to the service at {BaseUrl} timed out.", null, ex);
        }

        if (!resp.IsSuccessStatusCode)
        {
            string message = await ReadErrorAsync(resp).ConfigureAwait(false);
            var code = resp.StatusCode;
            resp.Dispose();
            throw new ServiceApiException(message, code);
        }
        return resp;
    }

    private static async Task<string> ReadErrorAsync(HttpResponseMessage resp)
    {
        string fallback = $"HTTP {(int)resp.StatusCode} {resp.ReasonPhrase}";
        string body;
        try { body = (await resp.Content.ReadAsStringAsync().ConfigureAwait(false)).Trim(); }
        catch { return fallback; }
        if (body.Length == 0) return fallback;

        string? extracted = null;
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind == JsonValueKind.Object)
            {
                foreach (var key in new[] { "message", "error", "detail", "title" })
                {
                    foreach (var prop in doc.RootElement.EnumerateObject())
                    {
                        if (prop.Name.Equals(key, StringComparison.OrdinalIgnoreCase)
                            && prop.Value.ValueKind == JsonValueKind.String
                            && !string.IsNullOrWhiteSpace(prop.Value.GetString()))
                        {
                            extracted = prop.Value.GetString();
                            break;
                        }
                    }
                    if (extracted != null) break;
                }
                if (extracted == null && doc.RootElement.TryGetProperty("errors", out var errs)) extracted = errs.ToString();
            }
        }
        catch (JsonException)
        {
            // plain-text body
        }

        string text = extracted ?? body;
        if (text.Length > 1000) text = text[..1000] + "...";
        return $"{fallback}: {text}";
    }

    private async Task<T> GetJsonAsync<T>(string url) where T : class
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        using var resp = await SendAsync(_http, req).ConfigureAwait(false);
        var result = await resp.Content.ReadFromJsonAsync<T>().ConfigureAwait(false);
        return result ?? throw new ServiceApiException("The service returned an empty response.");
    }

    private async Task<HttpResponseMessage> SendJsonAsync<TBody>(HttpMethod method, string url, TBody body)
    {
        using var req = new HttpRequestMessage(method, url) { Content = JsonContent.Create(body) };
        return await SendAsync(_http, req).ConfigureAwait(false);
    }

    /// <summary>Write operation: never falls back to the database; an offline service becomes a clear error.</summary>
    private async Task<HttpResponseMessage> SendWriteAsync<TBody>(HttpMethod method, string url, TBody? body) where TBody : class
    {
        using var req = new HttpRequestMessage(method, url);
        if (body != null) req.Content = JsonContent.Create(body);
        try { return await SendAsync(_http, req).ConfigureAwait(false); }
        catch (ServiceOfflineException ex) { throw new ServiceApiException(OfflineWriteMessage, null, ex); }
    }

    private Task<HttpResponseMessage> SendWriteAsync(HttpMethod method, string url) => SendWriteAsync<object>(method, url, null);

    private static string MessageFromJson(JsonElement doc)
    {
        if (doc.ValueKind == JsonValueKind.Object)
            foreach (var prop in doc.EnumerateObject())
                if (prop.Name.Equals("message", StringComparison.OrdinalIgnoreCase) && prop.Value.ValueKind == JsonValueKind.String)
                    return prop.Value.GetString() ?? string.Empty;
        return string.Empty;
    }

    private static bool HasMask(string? v) => v == SecretMask;

    private static void MaskSecrets(ComparisonJob job)
    {
        if (!string.IsNullOrEmpty(job.BucketA?.SecretKey)) job.BucketA!.SecretKey = SecretMask;
        if (!string.IsNullOrEmpty(job.BucketB?.SecretKey)) job.BucketB!.SecretKey = SecretMask;
        if (job.AlertConfig != null)
        {
            if (!string.IsNullOrEmpty(job.AlertConfig.SmtpPassword)) job.AlertConfig.SmtpPassword = SecretMask;
            if (!string.IsNullOrEmpty(job.AlertConfig.AzureWebhookUrl)) job.AlertConfig.AzureWebhookUrl = SecretMask;
            if (!string.IsNullOrEmpty(job.AlertConfig.AzureAppInsightsConnectionString)) job.AlertConfig.AzureAppInsightsConnectionString = SecretMask;
        }
    }

    // ---------- Status / presets ----------

    public async Task<ServiceStatusDto> GetStatusAsync()
    {
        try
        {
            return await GetJsonAsync<ServiceStatusDto>("/api/status").ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is ServiceOfflineException or ServiceApiException)
        {
            return new ServiceStatusDto
            {
                Service = "S3CompareService",
                Status = ex is ServiceOfflineException ? "Stopped / Offline" : "Unreachable",
                DatabasePath = DatabaseConfig.GetDefaultDatabasePath()
            };
        }
    }

    public async Task<List<S3EndpointPreset>> GetPresetsAsync()
    {
        try { return await GetJsonAsync<List<S3EndpointPreset>>("/api/presets").ConfigureAwait(false); }
        catch (ServiceOfflineException) { return S3CompareService.Core.Engine.S3ClientFactory.GetWasabiEndpoints(); }
    }

    public async Task<(bool Success, string Message)> TestConnectionAsync(S3BucketConfig config)
    {
        try
        {
            using var resp = await SendJsonAsync(HttpMethod.Post, "/api/test-connection", config).ConfigureAwait(false);
            var doc = await resp.Content.ReadFromJsonAsync<JsonElement>().ConfigureAwait(false);
            bool ok = doc.TryGetProperty("success", out var s) && s.ValueKind == JsonValueKind.True;
            return (ok, MessageFromJson(doc));
        }
        catch (ServiceApiException ex)
        {
            return (false, ex.Message);
        }
        catch (ServiceOfflineException)
        {
            if (HasMask(config.SecretKey))
                return (false, "The service is offline and the stored secret key cannot be used for a direct test. Start the service or re-enter the secret key.");
            return await S3CompareService.Core.Engine.S3ClientFactory.TestConnectionAsync(config);
        }
    }

    // ---------- Jobs ----------

    public async Task<List<ComparisonJob>> GetJobsAsync()
    {
        try
        {
            return await GetJsonAsync<List<ComparisonJob>>("/api/jobs").ConfigureAwait(false);
        }
        catch (ServiceOfflineException)
        {
            // Read-only fallback; secrets are masked client-side so they are never displayed or re-saved.
            await EnsureDirectDbInitializedAsync().ConfigureAwait(false);
            var jobs = await _directJobRepo.GetAllAsync().ConfigureAwait(false);
            foreach (var j in jobs) MaskSecrets(j);
            return jobs;
        }
    }

    /// <summary>Saves via the service only. Throws ServiceApiException (with the server's error text) on failure.</summary>
    public async Task SaveJobAsync(ComparisonJob job)
    {
        using var resp = await SendWriteAsync(HttpMethod.Put, $"/api/jobs/{Uri.EscapeDataString(job.Id)}", job).ConfigureAwait(false);
    }

    public async Task DeleteJobAsync(string id)
    {
        using var resp = await SendWriteAsync(HttpMethod.Delete, $"/api/jobs/{Uri.EscapeDataString(id)}").ConfigureAwait(false);
    }

    public async Task<(bool Success, string Message)> TriggerJobAsync(string jobId)
    {
        try
        {
            using var resp = await SendWriteAsync(HttpMethod.Post, $"/api/jobs/{Uri.EscapeDataString(jobId)}/run").ConfigureAwait(false);
            string msg = string.Empty;
            try { msg = MessageFromJson(await resp.Content.ReadFromJsonAsync<JsonElement>().ConfigureAwait(false)); } catch { }
            return (true, msg);
        }
        catch (ServiceApiException ex)
        {
            return (false, ex.Message);
        }
    }

    // ---------- Runs / differences / logs ----------

    public async Task<List<ScanRun>> GetRunsAsync(string? jobId = null, int count = 50)
    {
        try
        {
            string url = string.IsNullOrWhiteSpace(jobId) ? $"/api/runs?count={count}" : $"/api/runs?jobId={Uri.EscapeDataString(jobId)}&count={count}";
            return await GetJsonAsync<List<ScanRun>>(url).ConfigureAwait(false);
        }
        catch (ServiceOfflineException)
        {
            await EnsureDirectDbInitializedAsync().ConfigureAwait(false);
            return await _directScanRunRepo.GetRecentRunsAsync(count, jobId).ConfigureAwait(false);
        }
    }

    public async Task<List<BucketMetadataSnapshot>> GetSnapshotsAsync(string runId)
    {
        try
        {
            return await GetJsonAsync<List<BucketMetadataSnapshot>>($"/api/runs/{Uri.EscapeDataString(runId)}/snapshots").ConfigureAwait(false);
        }
        catch (ServiceOfflineException)
        {
            await EnsureDirectDbInitializedAsync().ConfigureAwait(false);
            return await _directScanRunRepo.GetMetadataSnapshotsAsync(runId).ConfigureAwait(false);
        }
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
            string url = $"/api/runs/{Uri.EscapeDataString(runId)}/differences?skip={skip}&take={take}";
            if (type.HasValue) url += $"&type={type.Value}";
            if (withinGracePeriod.HasValue) url += $"&withinGracePeriod={withinGracePeriod.Value}";
            if (!string.IsNullOrWhiteSpace(searchKey)) url += $"&searchKey={Uri.EscapeDataString(searchKey)}";

            return await GetJsonAsync<DifferencePageResult>(url).ConfigureAwait(false);
        }
        catch (ServiceOfflineException)
        {
            await EnsureDirectDbInitializedAsync().ConfigureAwait(false);
            var items = await _directDiffRepo.GetDifferencesAsync(runId, type, withinGracePeriod, searchKey, skip, take).ConfigureAwait(false);
            int total = await _directDiffRepo.GetDifferenceCountAsync(runId, type, withinGracePeriod, searchKey).ConfigureAwait(false);
            return new DifferencePageResult { TotalCount = total, Skip = skip, Take = take, Items = items };
        }
    }

    /// <summary>Streams the server-generated CSV/JSON export to <paramref name="destinationPath"/> (long timeout).</summary>
    public async Task ExportRunAsync(string runId, bool json, bool includeGracePeriod, string destinationPath)
    {
        string url = $"/api/runs/{Uri.EscapeDataString(runId)}/export/{(json ? "json" : "csv")}?includeGracePeriod={(includeGracePeriod ? "true" : "false")}";
        string tempPath = destinationPath + ".part";
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Get, url);
            using var resp = await SendAsync(_longHttp, req, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
            await using (var src = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false))
            await using (var dst = File.Create(tempPath))
            {
                await src.CopyToAsync(dst).ConfigureAwait(false);
            }
            File.Move(tempPath, destinationPath, overwrite: true);
        }
        catch (ServiceOfflineException ex)
        {
            throw new ServiceApiException("The service must be running to export reports.", null, ex);
        }
        finally
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
        }
    }

    public async Task<List<ServiceAuditLog>> GetLogsAsync(int count = 100)
    {
        try
        {
            return await GetJsonAsync<List<ServiceAuditLog>>($"/api/logs?count={count}").ConfigureAwait(false);
        }
        catch (ServiceOfflineException)
        {
            await EnsureDirectDbInitializedAsync().ConfigureAwait(false);
            return await _directAuditRepo.GetRecentLogsAsync(count).ConfigureAwait(false);
        }
    }

    // ---------- Scan controls ----------

    private async Task<(bool Success, string Message)> PostRunActionAsync(string runId, string action)
    {
        try
        {
            using var resp = await SendWriteAsync(HttpMethod.Post, $"/api/runs/{Uri.EscapeDataString(runId)}/{action}").ConfigureAwait(false);
            return (true, string.Empty);
        }
        catch (ServiceApiException ex)
        {
            return (false, ex.Message);
        }
    }

    public Task<(bool Success, string Message)> PauseRunAsync(string runId) => PostRunActionAsync(runId, "pause");
    public Task<(bool Success, string Message)> ResumeRunAsync(string runId) => PostRunActionAsync(runId, "resume");
    public Task<(bool Success, string Message)> CancelRunAsync(string runId) => PostRunActionAsync(runId, "cancel");

    // ---------- Alert tests ----------

    private static bool AlertHasMaskedSecrets(JobAlertConfig c) =>
        HasMask(c.SmtpPassword) || HasMask(c.AzureWebhookUrl) || HasMask(c.AzureAppInsightsConnectionString);

    private async Task<(bool Success, string Message)> TestAlertAsync(
        string endpoint,
        JobAlertConfig config,
        Func<S3CompareService.Core.Notifications.AlertNotificationService, Task<(bool Success, string Message)>> direct)
    {
        try
        {
            using var resp = await SendJsonAsync(HttpMethod.Post, endpoint, config).ConfigureAwait(false);
            var result = await resp.Content.ReadFromJsonAsync<AlertTestResponse>().ConfigureAwait(false);
            return result != null ? (result.Success, result.Message) : (false, "The service returned an empty response.");
        }
        catch (ServiceApiException ex)
        {
            return (false, ex.Message);
        }
        catch (ServiceOfflineException)
        {
            if (AlertHasMaskedSecrets(config))
                return (false, "The service is offline and stored secrets cannot be used for a direct test. Start the service or re-enter the secret values.");
            return await direct(new S3CompareService.Core.Notifications.AlertNotificationService());
        }
    }

    public Task<(bool Success, string Message)> TestSmtpAsync(JobAlertConfig config) =>
        TestAlertAsync("/api/alerts/test-smtp", config, s => s.TestSmtpConnectionAsync(config));

    public Task<(bool Success, string Message)> TestAzureWebhookAsync(JobAlertConfig config) =>
        TestAlertAsync("/api/alerts/test-azure-webhook", config, s => s.TestAzureWebhookAsync(config));

    public Task<(bool Success, string Message)> TestDailyDigestAsync(JobAlertConfig config, ComparisonJob job) =>
        TestAlertAsync("/api/alerts/test-daily-digest", config, s => s.SendTestDailyDigestAsync(config, job));
}

public class AlertTestResponse
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
}
