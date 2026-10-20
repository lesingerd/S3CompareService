using Cronos;
using Microsoft.AspNetCore.Mvc;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Notifications;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.Services;

var builder = WebApplication.CreateBuilder(args);

// ---- Configuration: S3Compare:Urls / S3Compare:DataDirectory (passed by the install script as command-line args) ----
const string ClientHeaderName = "X-S3Compare-Client";
const int MaxPageSize = 1000;

string urlsSetting = builder.Configuration["S3Compare:Urls"] ?? "http://localhost:5088";
var urls = urlsSetting.Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
if (urls.Length == 0) urls = new[] { "http://localhost:5088" };
foreach (var u in urls)
{
    // The management API is local-only: refuse anything that is not a loopback address.
    if (!Uri.TryCreate(u, UriKind.Absolute, out var uri) || !IsLoopbackHost(uri.Host))
        throw new InvalidOperationException($"S3Compare:Urls entry '{u}' is not allowed; the service only binds to localhost, 127.0.0.1 or [::1].");
}
builder.WebHost.UseUrls(urls);

string? dataDirectory = builder.Configuration["S3Compare:DataDirectory"];
if (!string.IsNullOrWhiteSpace(dataDirectory))
{
    DatabaseConfig.SetCustomDatabasePath(Path.Combine(dataDirectory.Trim().Trim('"'), "s3compareservice.db"));
}

// Defense in depth for DNS rebinding: Kestrel's host filtering plus the explicit middleware below.
builder.Configuration["AllowedHosts"] = "localhost;127.0.0.1;[::1]";

// Give active scans time to checkpoint on service stop (scheduler waits up to ~45s of this).
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(60));

// Enable running as native Windows Service or Console app
builder.Host.UseWindowsService(options =>
{
    options.ServiceName = "S3CompareService";
});

// Core Storage & Repositories
builder.Services.AddSingleton<SqliteConnectionFactory>();
builder.Services.AddSingleton<DatabaseMigrator>();
builder.Services.AddSingleton<IJobRepository, JobRepository>();
builder.Services.AddSingleton<IScanRunRepository, ScanRunRepository>();
builder.Services.AddSingleton<IDifferenceRepository, DifferenceRepository>();
builder.Services.AddSingleton<IAuditLogRepository, AuditLogRepository>();
builder.Services.AddSingleton<IAlertNotificationService, AlertNotificationService>();

// Engine & Coordination
builder.Services.AddSingleton<ScanExecutionCoordinator>();
builder.Services.AddTransient<CrashRecoveryManager>();
builder.Services.AddHostedService<ScanSchedulerService>();

var app = builder.Build();

// Initialize database schema and WAL mode, then recover interrupted scans. This runs before the server starts
// accepting requests (app.Run below), so the API can never race recovery.
using (var scope = app.Services.CreateScope())
{
    var migrator = scope.ServiceProvider.GetRequiredService<DatabaseMigrator>();
    var auditRepo = scope.ServiceProvider.GetRequiredService<IAuditLogRepository>();
    await migrator.MigrateAsync();
    await auditRepo.LogAsync("INFO", "ServiceHost", "Database migrations initialized successfully in WAL mode.");

    try
    {
        var recovery = scope.ServiceProvider.GetRequiredService<CrashRecoveryManager>();
        await recovery.RunStartupRecoveryAsync();
    }
    catch (Exception ex)
    {
        app.Logger.LogError(ex, "Error occurred during crash recovery startup sequence.");
    }
}

var startTime = DateTime.UtcNow;

// ==================== Security & error pipeline ====================

app.UseExceptionHandler(errorApp => errorApp.Run(async ctx =>
{
    ctx.Response.StatusCode = StatusCodes.Status500InternalServerError;
    await ctx.Response.WriteAsJsonAsync(new { message = "An internal error occurred." });
}));

// Reject requests whose Host header is not a loopback name (DNS rebinding).
app.Use(async (ctx, next) =>
{
    if (!IsLoopbackHost(ctx.Request.Host.Host))
    {
        ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
        await ctx.Response.WriteAsJsonAsync(new { message = "Invalid Host header." });
        return;
    }
    await next();
});

// A custom header on every API call forces browsers to issue a CORS preflight, which this service never
// approves (no CORS policy is configured), so cross-site pages cannot read or write the API.
app.Use(async (ctx, next) =>
{
    if (ctx.Request.Path.StartsWithSegments("/api") && !ctx.Request.Headers.ContainsKey(ClientHeaderName))
    {
        ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
        await ctx.Response.WriteAsJsonAsync(new { message = $"Missing required header '{ClientHeaderName}'." });
        return;
    }
    await next();
});

// Bodiless 4xx/5xx responses (404, 415, ...) get a JSON {"message": "..."} body.
app.UseStatusCodePages(async ctx =>
{
    var status = ctx.HttpContext.Response.StatusCode;
    ctx.HttpContext.Response.ContentType = "application/json";
    string message = status switch
    {
        StatusCodes.Status400BadRequest => "The request was invalid.",
        StatusCodes.Status404NotFound => "Not found.",
        StatusCodes.Status415UnsupportedMediaType => "Unsupported media type; send application/json.",
        _ => $"Request failed with status {status}."
    };
    await ctx.HttpContext.Response.WriteAsJsonAsync(new { message });
});

// ==================== REST API ENDPOINTS ====================

static bool IsLoopbackHost(string host)
{
    host = host.Trim('[', ']');
    return host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
        || host.Equals("127.0.0.1", StringComparison.Ordinal)
        || host.Equals("::1", StringComparison.Ordinal);
}

static IResult Error(int status, string message) => Results.Json(new { message }, statusCode: status);

static int Clamp(int value, int min, int max) => Math.Min(Math.Max(value, min), max);

static string? ValidateJob(ComparisonJob job)
{
    if (job.ScheduleType == JobScheduleType.CronExpression && !string.IsNullOrWhiteSpace(job.CronExpression))
    {
        try { CronExpression.Parse(job.CronExpression); }
        catch (Exception ex) { return $"Invalid cron expression: {ex.Message}"; }
    }
    return null;
}

// Persists a job from an API client: masked secrets keep their stored values, server-owned state is ignored.
static async Task<ComparisonJob> SaveJobFromClientAsync(ComparisonJob job, IJobRepository repo)
{
    var existing = await repo.GetByIdAsync(job.Id);
    SecretMask.MergeMaskedSecrets(job, existing);
    if (existing == null)
    {
        job.CreatedAtUtc = DateTime.UtcNow;
        job.LastRunTimeUtc = null;
        job.NextRunTimeUtc = null;
        job.LastRunStatus = null;
        job.AlertConfig.LastAlertSentUtc = null;
        job.AlertConfig.LastDailyDigestSentUtc = null;
    }
    await repo.SaveAsync(job); // existing rows keep server-owned fields (see IJobRepository.SaveAsync)
    return (await repo.GetByIdAsync(job.Id)) ?? job;
}

// 1. Status & Health
app.MapGet("/api/status", (ScanExecutionCoordinator coordinator) =>
{
    var activeScans = coordinator.GetActiveScans().Select(s => new
    {
        s.ScanRunId,
        s.JobId,
        s.JobName,
        s.StartTimeUtc,
        IsPaused = s.Engine.IsPaused,
        Duration = (DateTime.UtcNow - s.StartTimeUtc).ToString(@"hh\:mm\:ss"),
        s.LatestStats.ObjectsCompared,
        s.LatestStats.MatchedCount,
        s.LatestStats.TotalDifferences,
        s.LatestStats.FilteredDifferencesCount,
        s.LatestStats.InFlightDifferencesCount,
        s.LatestStats.ItemsPerSecond,
        s.LatestStats.CurrentKeyA,
        s.LatestStats.CurrentKeyB
    }).ToList();

    return Results.Ok(new
    {
        Service = "S3CompareService",
        Version = "1.0.0",
        Status = "Running",
        Uptime = (DateTime.UtcNow - startTime).ToString(@"d\.hh\:mm\:ss"),
        MemoryMB = Environment.WorkingSet / (1024.0 * 1024.0),
        DatabasePath = DatabaseConfig.GetDefaultDatabasePath(),
        ActiveScanCount = activeScans.Count,
        ActiveScans = activeScans
    });
});

// 2. Presets
app.MapGet("/api/presets", () => Results.Ok(S3ClientFactory.GetWasabiEndpoints()));

// 3. Test Connection. Masked secrets are resolved from the stored job named by ?jobId= (&bucket=A|B).
app.MapPost("/api/test-connection", async (
    [FromBody] S3BucketConfig config,
    IJobRepository jobs,
    [FromQuery] string? jobId,
    [FromQuery] string? bucket) =>
{
    if (SecretMask.IsMask(config.AccessKey) || SecretMask.IsMask(config.SecretKey))
    {
        var job = string.IsNullOrWhiteSpace(jobId) ? null : await jobs.GetByIdAsync(jobId);
        var stored = job == null ? null
            : string.Equals(bucket, "B", StringComparison.OrdinalIgnoreCase) ? job.BucketB : job.BucketA;
        if (stored == null)
            return Error(400, "The credentials are masked and no stored job was identified; re-enter the access key and secret key.");
        config.AccessKey = SecretMask.Merge(config.AccessKey, stored.AccessKey);
        config.SecretKey = SecretMask.Merge(config.SecretKey, stored.SecretKey);
    }

    var (success, message) = await S3ClientFactory.TestConnectionAsync(config);
    return Results.Ok(new { Success = success, Message = message });
});

// 4. List Buckets
app.MapPost("/api/list-buckets", async (
    [FromBody] S3BucketConfig config,
    IJobRepository jobs,
    [FromQuery] string? jobId,
    [FromQuery] string? bucket) =>
{
    if (SecretMask.IsMask(config.AccessKey) || SecretMask.IsMask(config.SecretKey))
    {
        var job = string.IsNullOrWhiteSpace(jobId) ? null : await jobs.GetByIdAsync(jobId);
        var stored = job == null ? null
            : string.Equals(bucket, "B", StringComparison.OrdinalIgnoreCase) ? job.BucketB : job.BucketA;
        if (stored == null)
            return Error(400, "The credentials are masked and no stored job was identified; re-enter the access key and secret key.");
        config.AccessKey = SecretMask.Merge(config.AccessKey, stored.AccessKey);
        config.SecretKey = SecretMask.Merge(config.SecretKey, stored.SecretKey);
    }

    var (success, buckets, message) = await S3ClientFactory.ListBucketsAsync(config);
    return Results.Ok(new { Success = success, Buckets = buckets, Message = message });
});

// 5. Jobs CRUD (secrets are always masked in responses)
app.MapGet("/api/jobs", async (IJobRepository repo) =>
{
    var jobs = await repo.GetAllAsync();
    return Results.Ok(jobs.Select(SecretMask.MaskedCopy).ToList());
});

app.MapGet("/api/jobs/{id}", async (string id, IJobRepository repo) =>
{
    var job = await repo.GetByIdAsync(id);
    return job != null ? Results.Ok(SecretMask.MaskedCopy(job)) : Error(404, $"Job '{id}' not found.");
});

app.MapPost("/api/jobs", async ([FromBody] ComparisonJob job, IJobRepository repo) =>
{
    if (string.IsNullOrWhiteSpace(job.Id))
    {
        job.Id = Guid.NewGuid().ToString("N");
    }
    var error = ValidateJob(job);
    if (error != null) return Error(400, error);

    var saved = await SaveJobFromClientAsync(job, repo);
    return Results.Created($"/api/jobs/{saved.Id}", SecretMask.MaskedCopy(saved));
});

// PUT is an upsert (the UI uses it for both create and update).
app.MapPut("/api/jobs/{id}", async (string id, [FromBody] ComparisonJob job, IJobRepository repo) =>
{
    job.Id = id;
    var error = ValidateJob(job);
    if (error != null) return Error(400, error);

    var saved = await SaveJobFromClientAsync(job, repo);
    return Results.Ok(SecretMask.MaskedCopy(saved));
});

app.MapDelete("/api/jobs/{id}", async (string id, IJobRepository repo, ScanExecutionCoordinator coordinator) =>
{
    if (coordinator.IsJobRunning(id))
        return Error(409, "The job has an active scan; cancel it before deleting the job.");
    await repo.DeleteAsync(id);
    return Results.NoContent();
});

// 6. Trigger Job Run
app.MapPost("/api/jobs/{id}/run", async (string id, ScanExecutionCoordinator coordinator) =>
{
    var result = await coordinator.TriggerScanAsync(id);
    if (result.Success)
    {
        return Results.Accepted($"/api/runs/{result.RunId}", new { Success = true, RunId = result.RunId, Message = result.Message });
    }
    return result.Failure switch
    {
        TriggerFailure.NotFound => Error(404, result.Message),
        TriggerFailure.ShuttingDown => Error(503, result.Message),
        _ => Error(409, result.Message)
    };
});

// 7. Runs History
app.MapGet("/api/runs", async (IScanRunRepository repo, [FromQuery] string? jobId, [FromQuery] int count = 50) =>
{
    var runs = await repo.GetRecentRunsAsync(Clamp(count, 1, MaxPageSize), jobId);
    return Results.Ok(runs);
});

app.MapGet("/api/runs/{id}", async (string id, IScanRunRepository repo) =>
{
    var run = await repo.GetByIdAsync(id);
    return run != null ? Results.Ok(run) : Error(404, $"Run '{id}' not found.");
});

app.MapGet("/api/runs/{id}/snapshots", async (string id, IScanRunRepository repo) =>
{
    var snapshots = await repo.GetMetadataSnapshotsAsync(id);
    return Results.Ok(snapshots);
});

// 8. Differences (Paged & Filtered)
app.MapGet("/api/runs/{id}/differences", async (
    string id,
    IDifferenceRepository repo,
    [FromQuery] DifferenceType? type,
    [FromQuery] bool? withinGracePeriod,
    [FromQuery] string? searchKey,
    [FromQuery] int skip = 0,
    [FromQuery] int take = 50) =>
{
    skip = Math.Max(skip, 0);
    take = Clamp(take, 1, MaxPageSize);
    var items = await repo.GetDifferencesAsync(id, type, withinGracePeriod, searchKey, skip, take);
    int totalCount = await repo.GetDifferenceCountAsync(id, type, withinGracePeriod, searchKey);
    return Results.Ok(new
    {
        TotalCount = totalCount,
        Skip = skip,
        Take = take,
        Items = items
    });
});

static string SanitizeFileNamePart(string input)
{
    if (string.IsNullOrWhiteSpace(input)) return "job";
    var invalid = Path.GetInvalidFileNameChars();
    return new string(input.Select(c => invalid.Contains(c) ? '_' : c).ToArray());
}

// 9. Export CSV (streamed straight to the response; never materialized in memory)
app.MapGet("/api/runs/{id}/export/csv", async (
    string id,
    HttpContext http,
    IScanRunRepository runRepo,
    IDifferenceRepository diffRepo,
    [FromQuery] bool includeGracePeriod = true) =>
{
    var run = await runRepo.GetByIdAsync(id);
    if (run == null) return Error(404, $"Run '{id}' not found.");

    string shortId = run.Id.Length >= 8 ? run.Id[..8] : run.Id;
    string filename = $"s3compare_report_{SanitizeFileNamePart(run.JobName)}_{shortId}.csv";
    var ct = http.RequestAborted;
    return Results.Stream(
        stream => ReportExporter.ExportToCsvStreamAsync(stream, run, diffRepo.StreamAllForRunAsync(id, includeGracePeriod ? null : false, ct), includeGracePeriod, ct),
        "text/csv",
        filename);
});

// 10. Export JSON
app.MapGet("/api/runs/{id}/export/json", async (
    string id,
    HttpContext http,
    IScanRunRepository runRepo,
    IDifferenceRepository diffRepo,
    [FromQuery] bool includeGracePeriod = true) =>
{
    var run = await runRepo.GetByIdAsync(id);
    if (run == null) return Error(404, $"Run '{id}' not found.");

    var snapshots = await runRepo.GetMetadataSnapshotsAsync(id);
    string shortId = run.Id.Length >= 8 ? run.Id[..8] : run.Id;
    string filename = $"s3compare_report_{SanitizeFileNamePart(run.JobName)}_{shortId}.json";
    var ct = http.RequestAborted;
    return Results.Stream(
        stream => ReportExporter.ExportToJsonStreamAsync(stream, run, diffRepo.StreamAllForRunAsync(id, includeGracePeriod ? null : false, ct), snapshots, includeGracePeriod, ct),
        "application/json",
        filename);
});

// 11. Run Controls (Pause, Resume, Cancel): 404 when the run is not active, 409 when the state does not allow it
app.MapPost("/api/runs/{id}/pause", (string id, ScanExecutionCoordinator coordinator) =>
{
    var ctx = coordinator.GetActiveScan(id);
    if (ctx == null) return Error(404, $"Run '{id}' is not active.");
    if (ctx.Engine.IsPaused) return Error(409, "The run is already paused.");
    return coordinator.PauseScan(id) ? Results.Ok(new { Status = "Paused" }) : Error(404, $"Run '{id}' is not active.");
});

app.MapPost("/api/runs/{id}/resume", (string id, ScanExecutionCoordinator coordinator) =>
{
    var ctx = coordinator.GetActiveScan(id);
    if (ctx == null) return Error(404, $"Run '{id}' is not active.");
    if (!ctx.Engine.IsPaused) return Error(409, "The run is not paused.");
    return coordinator.ResumeScan(id) ? Results.Ok(new { Status = "Resumed" }) : Error(404, $"Run '{id}' is not active.");
});

app.MapPost("/api/runs/{id}/cancel", (string id, ScanExecutionCoordinator coordinator) =>
{
    if (coordinator.IsStopping) return Error(409, "The service is shutting down; the run will be resumed at next start.");
    return coordinator.CancelScan(id) ? Results.Ok(new { Status = "Cancelling" }) : Error(404, $"Run '{id}' is not active.");
});

// 12. Audit Logs
app.MapGet("/api/logs", async (IAuditLogRepository repo, [FromQuery] int count = 100, [FromQuery] string? level = null) =>
{
    var logs = await repo.GetRecentLogsAsync(Clamp(count, 1, MaxPageSize), level);
    return Results.Ok(logs);
});

// 13. Alert Testing. Masked secrets are resolved from the stored job named by ?jobId=.
static async Task<IResult?> UnmaskAlertAsync(JobAlertConfig config, string? jobId, IJobRepository jobs)
{
    if (!SecretMask.IsMask(config.SmtpPassword) && !SecretMask.IsMask(config.AzureWebhookUrl) && !SecretMask.IsMask(config.AzureAppInsightsConnectionString))
        return null;
    var job = string.IsNullOrWhiteSpace(jobId) ? null : await jobs.GetByIdAsync(jobId);
    if (job == null)
        return Error(400, "The secrets are masked and no stored job was identified; re-enter the secret values.");
    SecretMask.MergeMaskedSecrets(config, job.AlertConfig);
    return null;
}

app.MapPost("/api/alerts/test-smtp", async ([FromBody] JobAlertConfig config, IAlertNotificationService alertService, IJobRepository jobs, [FromQuery] string? jobId) =>
{
    var err = await UnmaskAlertAsync(config, jobId, jobs);
    if (err != null) return err;
    var (success, message) = await alertService.TestSmtpConnectionAsync(config);
    return Results.Ok(new { Success = success, Message = message });
});

app.MapPost("/api/alerts/test-azure-webhook", async ([FromBody] JobAlertConfig config, IAlertNotificationService alertService, IJobRepository jobs, [FromQuery] string? jobId) =>
{
    var err = await UnmaskAlertAsync(config, jobId, jobs);
    if (err != null) return err;
    var (success, message) = await alertService.TestAzureWebhookAsync(config);
    return Results.Ok(new { Success = success, Message = message });
});

app.MapPost("/api/alerts/test-daily-digest", async ([FromBody] JobAlertConfig config, IAlertNotificationService alertService, IJobRepository jobs, [FromQuery] string? jobId) =>
{
    var err = await UnmaskAlertAsync(config, jobId, jobs);
    if (err != null) return err;
    var sampleJob = new ComparisonJob
    {
        Name = "Test Daily Digest Job",
        AlertConfig = config
    };
    var (success, message) = await alertService.SendTestDailyDigestAsync(config, sampleJob);
    return Results.Ok(new { Success = success, Message = message });
});

app.Run();

/// <summary>Entry-point marker so integration tests can use WebApplicationFactory&lt;Program&gt;.</summary>
public partial class Program { }
