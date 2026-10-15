using System.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.Services;

var builder = WebApplication.CreateBuilder(args);

// Enable running as native Windows Service or Console app
builder.Host.UseWindowsService(options =>
{
    options.ServiceName = "S3CompareService";
});

// Configure Kestrel to listen on port 5088 (or configured URL)
builder.WebHost.ConfigureKestrel(options =>
{
    // Default to port 5088 for local Windows UI & optional Web UI
    if (!builder.Configuration.GetSection("Kestrel:Endpoints").Exists())
    {
        options.ListenLocalhost(5088);
    }
});

// Configure CORS for web UI access
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAll", policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyHeader()
              .AllowAnyMethod();
    });
});

// Core Storage & Repositories
builder.Services.AddSingleton<SqliteConnectionFactory>();
builder.Services.AddSingleton<DatabaseMigrator>();
builder.Services.AddSingleton<IJobRepository, JobRepository>();
builder.Services.AddSingleton<IScanRunRepository, ScanRunRepository>();
builder.Services.AddSingleton<IDifferenceRepository, DifferenceRepository>();
builder.Services.AddSingleton<IAuditLogRepository, AuditLogRepository>();

// Engine & Coordination
builder.Services.AddSingleton<ScanExecutionCoordinator>();
builder.Services.AddTransient<CrashRecoveryManager>();
builder.Services.AddHostedService<ScanSchedulerService>();

var app = builder.Build();

app.UseCors("AllowAll");

// Initialize database schema and WAL mode
using (var scope = app.Services.CreateScope())
{
    var migrator = scope.ServiceProvider.GetRequiredService<DatabaseMigrator>();
    var auditRepo = scope.ServiceProvider.GetRequiredService<IAuditLogRepository>();
    await migrator.MigrateAsync();
    await auditRepo.LogAsync("INFO", "ServiceHost", "Database migrations initialized successfully in WAL mode.");
}

var startTime = DateTime.UtcNow;

// ==================== REST API ENDPOINTS ====================

// 1. Status & Health
app.MapGet("/api/status", (ScanExecutionCoordinator coordinator) =>
{
    var proc = Process.GetCurrentProcess();
    var activeScans = coordinator.GetActiveScans().Select(s => new
    {
        s.ScanRunId,
        s.JobId,
        s.JobName,
        s.StartTimeUtc,
        Duration = (DateTime.UtcNow - s.StartTimeUtc).ToString(@"hh\:mm\:ss"),
        s.LatestStats.ObjectsCompared,
        s.LatestStats.MatchedCount,
        s.LatestStats.TotalDifferences,
        s.LatestStats.FilteredDifferencesCount,
        s.LatestStats.InFlightDifferencesCount,
        s.LatestStats.ItemsPerSecond,
        s.LatestStats.CurrentKeyA,
        s.LatestStats.CurrentKeyB
    });

    return Results.Ok(new
    {
        Service = "S3CompareService",
        Version = "1.0.0",
        Status = "Running",
        Uptime = (DateTime.UtcNow - startTime).ToString(@"d\.hh\:mm\:ss"),
        MemoryMB = proc.WorkingSet64 / (1024.0 * 1024.0),
        DatabasePath = DatabaseConfig.GetDefaultDatabasePath(),
        ActiveScanCount = coordinator.GetActiveScans().Count(),
        ActiveScans = activeScans
    });
});

// 2. Presets
app.MapGet("/api/presets", () => Results.Ok(S3ClientFactory.GetWasabiEndpoints()));

// 3. Test Connection
app.MapPost("/api/test-connection", async ([FromBody] S3BucketConfig config) =>
{
    var (success, message) = await S3ClientFactory.TestConnectionAsync(config);
    return Results.Ok(new { Success = success, Message = message });
});

// 4. List Buckets
app.MapPost("/api/list-buckets", async ([FromBody] S3BucketConfig config) =>
{
    var (success, buckets, message) = await S3ClientFactory.ListBucketsAsync(config);
    return Results.Ok(new { Success = success, Buckets = buckets, Message = message });
});

// 5. Jobs CRUD
app.MapGet("/api/jobs", async (IJobRepository repo) =>
{
    var jobs = await repo.GetAllAsync();
    return Results.Ok(jobs);
});

app.MapGet("/api/jobs/{id}", async (string id, IJobRepository repo) =>
{
    var job = await repo.GetByIdAsync(id);
    return job != null ? Results.Ok(job) : Results.NotFound();
});

app.MapPost("/api/jobs", async ([FromBody] ComparisonJob job, IJobRepository repo) =>
{
    if (string.IsNullOrWhiteSpace(job.Id))
    {
        job.Id = Guid.NewGuid().ToString("N");
    }
    job.CreatedAtUtc = DateTime.UtcNow;
    await repo.SaveAsync(job);
    return Results.Created($"/api/jobs/{job.Id}", job);
});

app.MapPut("/api/jobs/{id}", async (string id, [FromBody] ComparisonJob job, IJobRepository repo) =>
{
    job.Id = id;
    await repo.SaveAsync(job);
    return Results.Ok(job);
});

app.MapDelete("/api/jobs/{id}", async (string id, IJobRepository repo) =>
{
    await repo.DeleteAsync(id);
    return Results.NoContent();
});

// 6. Trigger Job Run
app.MapPost("/api/jobs/{id}/run", async (string id, ScanExecutionCoordinator coordinator) =>
{
    var (success, runId, message) = await coordinator.TriggerScanAsync(id);
    if (!success)
    {
        return Results.Conflict(new { Success = false, RunId = runId, Message = message });
    }
    return Results.Accepted($"/api/runs/{runId}", new { Success = true, RunId = runId, Message = message });
});

// 7. Runs History
app.MapGet("/api/runs", async (IScanRunRepository repo, [FromQuery] string? jobId, [FromQuery] int count = 50) =>
{
    var runs = await repo.GetRecentRunsAsync(count, jobId);
    return Results.Ok(runs);
});

app.MapGet("/api/runs/{id}", async (string id, IScanRunRepository repo) =>
{
    var run = await repo.GetByIdAsync(id);
    return run != null ? Results.Ok(run) : Results.NotFound();
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

// 9. Export CSV
app.MapGet("/api/runs/{id}/export/csv", async (
    string id,
    IScanRunRepository runRepo,
    IDifferenceRepository diffRepo,
    [FromQuery] bool includeGracePeriod = true) =>
{
    var run = await runRepo.GetByIdAsync(id);
    if (run == null) return Results.NotFound();

    var diffs = await diffRepo.GetAllForRunAsync(id, includeGracePeriod ? null : false);
    var memStream = new MemoryStream();
    var writer = new StreamWriter(memStream, leaveOpen: true);
    await ReportExporter.ExportToCsvAsync(writer, run, diffs, includeGracePeriod);
    memStream.Position = 0;

    string filename = $"s3compare_report_{run.JobName}_{run.Id[..8]}.csv";
    return Results.File(memStream, "text/csv", filename);
});

// 10. Export JSON
app.MapGet("/api/runs/{id}/export/json", async (
    string id,
    IScanRunRepository runRepo,
    IDifferenceRepository diffRepo,
    [FromQuery] bool includeGracePeriod = true) =>
{
    var run = await runRepo.GetByIdAsync(id);
    if (run == null) return Results.NotFound();

    var diffs = await diffRepo.GetAllForRunAsync(id, includeGracePeriod ? null : false);
    var snapshots = await runRepo.GetMetadataSnapshotsAsync(id);

    var memStream = new MemoryStream();
    await ReportExporter.ExportToJsonAsync(memStream, run, diffs, snapshots, includeGracePeriod);
    memStream.Position = 0;

    string filename = $"s3compare_report_{run.JobName}_{run.Id[..8]}.json";
    return Results.File(memStream, "application/json", filename);
});

// 11. Run Controls (Pause, Resume, Cancel)
app.MapPost("/api/runs/{id}/pause", (string id, ScanExecutionCoordinator coordinator) =>
{
    bool paused = coordinator.PauseScan(id);
    return paused ? Results.Ok(new { Status = "Paused" }) : Results.NotFound();
});

app.MapPost("/api/runs/{id}/resume", (string id, ScanExecutionCoordinator coordinator) =>
{
    bool resumed = coordinator.ResumeScan(id);
    return resumed ? Results.Ok(new { Status = "Resumed" }) : Results.NotFound();
});

app.MapPost("/api/runs/{id}/cancel", (string id, ScanExecutionCoordinator coordinator) =>
{
    bool cancelled = coordinator.CancelScan(id);
    return cancelled ? Results.Ok(new { Status = "Cancelling" }) : Results.NotFound();
});

// 12. Audit Logs
app.MapGet("/api/logs", async (IAuditLogRepository repo, [FromQuery] int count = 100, [FromQuery] string? level = null) =>
{
    var logs = await repo.GetRecentLogsAsync(count, level);
    return Results.Ok(logs);
});

app.Run();
