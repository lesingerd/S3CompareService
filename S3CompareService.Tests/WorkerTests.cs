using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Core.Models;
using S3CompareService.Core.Notifications;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.Services;

namespace S3CompareService.Tests;

[TestClass]
public class WorkerApiTests
{
    private string _dataDir = string.Empty;
    private WebApplicationFactory<Program> _factory = null!;
    private HttpClient _client = null!;

    [TestInitialize]
    public void Setup()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"s3worker_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dataDir);
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("S3Compare:DataDirectory", _dataDir);
            b.UseSetting("S3Compare:Urls", "http://localhost:5088");
        });
        _client = _factory.CreateClient();
        _client.DefaultRequestHeaders.Add("X-S3Compare-Client", "ui");
    }

    [TestCleanup]
    public void Cleanup()
    {
        _client.Dispose();
        _factory.Dispose();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dataDir, true); } catch { }
    }

    private static ComparisonJob NewJob(string id) => new()
    {
        Id = id,
        Name = "api job",
        IsEnabled = false,
        BucketA = { BucketName = "a", AccessKey = "AKIA-A", SecretKey = "secret-A" },
        BucketB = { BucketName = "b", AccessKey = "AKIA-B", SecretKey = "secret-B" },
        AlertConfig = { SmtpPassword = "smtp-pw" }
    };

    [TestMethod]
    public async Task Api_WithoutClientHeader_Is403()
    {
        using var bare = _factory.CreateClient();
        var resp = await bare.GetAsync("/api/status");
        Assert.AreEqual(HttpStatusCode.Forbidden, resp.StatusCode);

        resp = await _client.GetAsync("/api/status");
        Assert.AreEqual(HttpStatusCode.OK, resp.StatusCode);
    }

    [TestMethod]
    public async Task Api_ForeignHostHeader_IsRejected()
    {
        var req = new HttpRequestMessage(HttpMethod.Get, "/api/status");
        req.Headers.Host = "evil.example.com";
        req.Headers.Add("X-S3Compare-Client", "ui");
        var resp = await _client.SendAsync(req);
        Assert.IsTrue(resp.StatusCode is HttpStatusCode.Forbidden or HttpStatusCode.BadRequest, "foreign Host must be rejected");
    }

    [TestMethod]
    public async Task Api_NoCorsHeadersAreEmitted()
    {
        var req = new HttpRequestMessage(HttpMethod.Get, "/api/status");
        req.Headers.Add("Origin", "http://evil.example.com");
        var resp = await _client.SendAsync(req);
        Assert.IsFalse(resp.Headers.Contains("Access-Control-Allow-Origin"));
    }

    [TestMethod]
    public async Task Jobs_AreMaskedOnGet_AndMaskedSecretsKeepStoredValuesOnPut()
    {
        var put = await _client.PutAsJsonAsync("/api/jobs/j1", NewJob("j1"));
        Assert.AreEqual(HttpStatusCode.OK, put.StatusCode);
        var putBody = await put.Content.ReadAsStringAsync();
        Assert.IsFalse(putBody.Contains("secret-A") || putBody.Contains("smtp-pw"), "PUT response leaked a secret");

        var raw = await _client.GetStringAsync("/api/jobs/j1");
        Assert.IsFalse(raw.Contains("secret-A") || raw.Contains("AKIA-A") || raw.Contains("smtp-pw"), "GET leaked a secret");
        Assert.IsTrue(raw.Contains(SecretMask.Mask));
        var all = await _client.GetStringAsync("/api/jobs");
        Assert.IsFalse(all.Contains("secret-B"), "list leaked a secret");

        // Round-trip the masked job and change the name: stored secrets must survive.
        var masked = (await _client.GetFromJsonAsync<ComparisonJob>("/api/jobs/j1"))!;
        masked.Name = "renamed";
        masked.LastRunStatus = RunStatus.Failed; // server-owned: must be ignored
        Assert.AreEqual(HttpStatusCode.OK, (await _client.PutAsJsonAsync("/api/jobs/j1", masked)).StatusCode);

        var repo = new JobRepository(new SqliteConnectionFactory(DatabaseConfig.GetConnectionString()));
        var stored = (await repo.GetByIdAsync("j1"))!;
        Assert.AreEqual("renamed", stored.Name);
        Assert.AreEqual("secret-A", stored.BucketA.SecretKey);
        Assert.AreEqual("AKIA-B", stored.BucketB.AccessKey);
        Assert.AreEqual("smtp-pw", stored.AlertConfig.SmtpPassword);
        Assert.IsNull(stored.LastRunStatus);
    }

    [TestMethod]
    public async Task Errors_AreJsonWithMessage_AndRunControlsReturn404()
    {
        var resp = await _client.PostAsync("/api/runs/nope/cancel", null);
        Assert.AreEqual(HttpStatusCode.NotFound, resp.StatusCode);
        var doc = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.IsFalse(string.IsNullOrWhiteSpace(doc.GetProperty("message").GetString()));

        resp = await _client.PostAsync("/api/jobs/missing/run", null);
        Assert.AreEqual(HttpStatusCode.NotFound, resp.StatusCode);
        resp = await _client.GetAsync("/api/runs/missing");
        Assert.AreEqual(HttpStatusCode.NotFound, resp.StatusCode);
        Assert.IsTrue((await resp.Content.ReadFromJsonAsync<JsonElement>()).TryGetProperty("message", out _));
    }

    [TestMethod]
    public async Task TestConnection_WithMaskedSecretAndNoJob_Is400()
    {
        var cfg = new S3BucketConfig { BucketName = "x", AccessKey = SecretMask.Mask, SecretKey = SecretMask.Mask };
        var resp = await _client.PostAsJsonAsync("/api/test-connection", cfg);
        Assert.AreEqual(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [TestMethod]
    public async Task Differences_TakeIsCapped()
    {
        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = "j", JobName = "j" };
        await new ScanRunRepository(new SqliteConnectionFactory(DatabaseConfig.GetConnectionString())).CreateAsync(run);
        var doc = await _client.GetFromJsonAsync<JsonElement>($"/api/runs/{run.Id}/differences?take=999999");
        Assert.AreEqual(1000, doc.GetProperty("take").GetInt32());
    }

    [TestMethod]
    public async Task Export_StreamsCsvWithDisposition()
    {
        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = "j", JobName = "my job" };
        await new ScanRunRepository(new SqliteConnectionFactory(DatabaseConfig.GetConnectionString())).CreateAsync(run);
        var resp = await _client.GetAsync($"/api/runs/{run.Id}/export/csv");
        Assert.AreEqual(HttpStatusCode.OK, resp.StatusCode);
        Assert.AreEqual("text/csv", resp.Content.Headers.ContentType?.MediaType);
        Assert.IsNotNull(resp.Content.Headers.ContentDisposition?.FileName);
        Assert.IsTrue((await resp.Content.ReadAsStringAsync()).Contains("Id,Key,Type"));
    }
}

[TestClass]
public class SchedulerLogicTests
{
    private static readonly DateTime Now = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    [TestMethod]
    public void Interval_NeverRun_IsDue()
    {
        var job = new ComparisonJob { ScheduleType = JobScheduleType.IntervalMinutes, IntervalMinutes = 30 };
        Assert.IsTrue(ScanSchedulerService.ComputeSchedule(job, Now).IsDue);
    }

    [TestMethod]
    public void Interval_AnchorsOnLastStart_NotCompletion()
    {
        var job = new ComparisonJob
        {
            ScheduleType = JobScheduleType.IntervalMinutes,
            IntervalMinutes = 60,
            LastRunTimeUtc = Now.AddMinutes(-5) // finished 5 minutes ago...
        };
        var started = Now.AddMinutes(-65);      // ...but started 65 minutes ago
        var (due, next) = ScanSchedulerService.ComputeSchedule(job, Now, started);
        Assert.IsTrue(due);
        Assert.AreEqual(started.AddMinutes(60), next);

        (due, _) = ScanSchedulerService.ComputeSchedule(job, Now);
        Assert.IsFalse(due);
    }

    [TestMethod]
    public void Cron_UsesUtcAndLastRun()
    {
        var job = new ComparisonJob
        {
            ScheduleType = JobScheduleType.CronExpression,
            CronExpression = "0 * * * *",
            LastRunTimeUtc = new DateTime(2026, 1, 1, 10, 30, 0, DateTimeKind.Utc)
        };
        var (due, next) = ScanSchedulerService.ComputeSchedule(job, Now);
        Assert.IsTrue(due);
        Assert.AreEqual(new DateTime(2026, 1, 1, 11, 0, 0, DateTimeKind.Utc), next);

        job.LastRunTimeUtc = DateTime.SpecifyKind(new DateTime(2026, 1, 1, 12, 0, 0), DateTimeKind.Unspecified);
        (due, next) = ScanSchedulerService.ComputeSchedule(job, Now);
        Assert.IsFalse(due);
        Assert.AreEqual(new DateTime(2026, 1, 1, 13, 0, 0, DateTimeKind.Utc), next);
    }
}

[TestClass]
public class RecoveryResumeTests : TestDbBase
{
    [TestMethod]
    public async Task InterruptedRun_IsResumedIntoSameRunId()
    {
        var job = new ComparisonJob
        {
            Name = "resume job",
            IsEnabled = true,
            BucketA = { BucketName = "a", EndpointUrl = "http://127.0.0.1:1", AccessKey = "k", SecretKey = "s" },
            BucketB = { BucketName = "b", EndpointUrl = "http://127.0.0.1:1", AccessKey = "k", SecretKey = "s" }
        };
        await JobRepo.SaveAsync(job);

        var run = new ScanRun
        {
            Id = Guid.NewGuid().ToString("N"),
            JobId = job.Id,
            JobName = job.Name,
            Status = RunStatus.Interrupted,
            EndTimeUtc = DateTime.UtcNow,
            LastEvaluatedKey = "k/0042"
        };
        await RunRepo.CreateAsync(run);
        await RunRepo.CompleteAsync(run);
        await RunRepo.SaveCheckpointAsync(new ScanCheckpoint
        {
            ScanRunId = run.Id,
            JobId = job.Id,
            LastEvaluatedKey = "k/0042",
            BucketNameA = "a",
            BucketNameB = "b",
            EndpointA = "http://127.0.0.1:1",
            EndpointB = "http://127.0.0.1:1"
        });

        var alerts = new AlertNotificationService(AuditRepo);
        var coordinator = new ScanExecutionCoordinator(JobRepo, RunRepo, DiffRepo, AuditRepo, alerts, NullLogger<ScanExecutionCoordinator>.Instance);
        var recovery = new CrashRecoveryManager(RunRepo, JobRepo, AuditRepo, coordinator, NullLogger<CrashRecoveryManager>.Instance);

        await recovery.RunStartupRecoveryAsync();

        // Same run id is active; no second run was created.
        Assert.IsTrue(coordinator.IsRunActive(run.Id));
        var all = await RunRepo.GetRecentRunsAsync(10);
        Assert.AreEqual(1, all.Count);
        Assert.AreEqual(run.Id, all[0].Id);
        var reopened = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.IsTrue(reopened.IsResumedFromCheckpoint);
        Assert.AreEqual(1, reopened.ResumeCount);

        // A second recovery pass must skip the run the coordinator holds.
        await recovery.RunStartupRecoveryAsync();
        Assert.AreEqual(1, (await RunRepo.GetRecentRunsAsync(10)).Count);

        coordinator.CancelScan(run.Id);
        await coordinator.StopAllActiveScansAsync(timeout: TimeSpan.FromSeconds(20));
        Assert.IsTrue(coordinator.IsStopping);
        var refused = await coordinator.TriggerScanAsync(job.Id);
        Assert.AreEqual(TriggerFailure.ShuttingDown, refused.Failure);
    }
}
