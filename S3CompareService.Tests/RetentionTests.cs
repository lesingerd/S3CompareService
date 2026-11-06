using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.Services;

namespace S3CompareService.Tests;

[TestClass]
public class RetentionTests : TestDbBase
{
    private static readonly DateTime Now = new(2026, 11, 6, 12, 0, 0, DateTimeKind.Utc);
    private RetentionManager _retention = null!;

    [TestInitialize]
    public void RetentionInit() => _retention = new RetentionManager(Factory);

    private static RetentionOptions Options(int keepRuns = 0, int keepDays = 0, int auditDays = 0, int batch = 2000) =>
        new() { KeepRunsPerJob = keepRuns, KeepDays = keepDays, AuditLogDays = auditDays, BatchSize = batch, BatchPause = TimeSpan.Zero };

    private async Task<ScanRun> AddRunAsync(string jobId, RunStatus status, TimeSpan age, int differences = 0, bool snapshot = true, bool checkpoint = false)
    {
        var start = Now - age;
        var run = new ScanRun
        {
            JobId = jobId, JobName = "job", Status = status, StartTimeUtc = start,
            EndTimeUtc = status is RunStatus.Running or RunStatus.Paused or RunStatus.Pending ? null : start.AddMinutes(5)
        };
        await RunRepo.CreateAsync(run);
        if (differences > 0)
        {
            await DiffRepo.InsertBatchAsync(Enumerable.Range(0, differences).Select(i => new RecordedDifference
            {
                ScanRunId = run.Id, Key = $"k/{i:D6}", Type = DifferenceType.MissingInBucketB, Details = "x", DetectedAtUtc = start
            }).ToList());
        }

        if (snapshot)
        {
            await RunRepo.SaveMetadataSnapshotAsync(new BucketMetadataSnapshot { ScanRunId = run.Id, BucketSide = "A", BucketName = "b", EndpointUrl = "e", SnapshotTimeUtc = start });
            await RunRepo.SaveMetadataSnapshotAsync(new BucketMetadataSnapshot { ScanRunId = run.Id, BucketSide = "B", BucketName = "b", EndpointUrl = "e", SnapshotTimeUtc = start });
        }

        if (checkpoint)
            await RunRepo.SaveCheckpointAsync(new ScanCheckpoint { ScanRunId = run.Id, JobId = jobId, LastEvaluatedKey = "k" });
        return run;
    }

    private Task<long> CountAsync(string table, string? runId = null) =>
        RawScalarAsync<long>(runId == null ? $"SELECT COUNT(*) FROM {table}" : $"SELECT COUNT(*) FROM {table} WHERE ScanRunId = @r", new { r = runId });

    private async Task<bool> RunExistsAsync(ScanRun run) => await RawScalarAsync<long>("SELECT COUNT(*) FROM ScanRuns WHERE Id = @Id", new { run.Id }) == 1;

    [TestMethod]
    public async Task KeepRunsPerJob_DeletesTheOldestRuns_WithTheirDifferencesAndSnapshots()
    {
        var runs = new List<ScanRun>();
        for (int day = 6; day >= 1; day--) runs.Add(await AddRunAsync("jobA", RunStatus.Completed, TimeSpan.FromDays(day), differences: 5));
        var other = await AddRunAsync("jobB", RunStatus.Completed, TimeSpan.FromDays(30), differences: 2);

        var result = await _retention.RunOnceAsync(Options(keepRuns: 3), Now);

        Assert.AreEqual(3, result.RunsDeleted);
        Assert.AreEqual(15, result.DifferencesDeleted);
        Assert.AreEqual(6, result.SnapshotsDeleted);
        for (int i = 0; i < 3; i++)
        {
            Assert.IsFalse(await RunExistsAsync(runs[i]), $"run {i} (old) is gone");
            Assert.AreEqual(0, await CountAsync("RecordedDifferences", runs[i].Id));
            Assert.AreEqual(0, await CountAsync("BucketMetadataSnapshots", runs[i].Id));
        }

        for (int i = 3; i < 6; i++)
        {
            Assert.IsTrue(await RunExistsAsync(runs[i]));
            Assert.AreEqual(5, await CountAsync("RecordedDifferences", runs[i].Id));
            Assert.AreEqual(2, await CountAsync("BucketMetadataSnapshots", runs[i].Id));
        }

        Assert.IsTrue(await RunExistsAsync(other), "another job has its own count");
        Assert.AreEqual(2, await CountAsync("RecordedDifferences", other.Id));
    }

    [TestMethod]
    public async Task KeepDays_DeletesRunsOlderThanTheLimit()
    {
        var recent = await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(1));
        var middle = await AddRunAsync("j", RunStatus.Failed, TimeSpan.FromDays(5));
        var old = await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(20), differences: 3);
        var older = await AddRunAsync("j", RunStatus.Cancelled, TimeSpan.FromDays(30));

        var result = await _retention.RunOnceAsync(Options(keepDays: 10), Now);

        Assert.AreEqual(2, result.RunsDeleted);
        Assert.IsTrue(await RunExistsAsync(recent));
        Assert.IsTrue(await RunExistsAsync(middle));
        Assert.IsFalse(await RunExistsAsync(old));
        Assert.IsFalse(await RunExistsAsync(older));
        Assert.AreEqual(0, await CountAsync("RecordedDifferences"));
    }

    [TestMethod]
    public async Task TheNewestCompletedRun_IsNeverDeleted_EvenWhenOldAndBeyondTheCount()
    {
        var completed = await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(300), differences: 4);
        var olderCompleted = await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(310));
        for (int i = 3; i >= 1; i--) await AddRunAsync("j", RunStatus.Failed, TimeSpan.FromDays(i));

        var result = await _retention.RunOnceAsync(Options(keepRuns: 2, keepDays: 30), Now);

        Assert.IsTrue(await RunExistsAsync(completed), "newest completed run survives the count and the age rule");
        Assert.AreEqual(4, await CountAsync("RecordedDifferences", completed.Id));
        Assert.IsFalse(await RunExistsAsync(olderCompleted), "an older completed run does not");
        Assert.AreEqual(2, result.RunsDeleted, "the older completed run and the oldest failed run");
    }

    [TestMethod]
    public async Task ResumableRuns_AndRunsWithACheckpoint_AreKept()
    {
        var running = await AddRunAsync("j", RunStatus.Running, TimeSpan.FromDays(400), differences: 2);
        var paused = await AddRunAsync("j", RunStatus.Paused, TimeSpan.FromDays(400));
        var interrupted = await AddRunAsync("j", RunStatus.Interrupted, TimeSpan.FromDays(400), differences: 2);
        var pending = await AddRunAsync("j", RunStatus.Pending, TimeSpan.FromDays(400));
        var failedWithCheckpoint = await AddRunAsync("j", RunStatus.Failed, TimeSpan.FromDays(400), differences: 2, checkpoint: true);
        var failedPlain = await AddRunAsync("j", RunStatus.Failed, TimeSpan.FromDays(400));
        await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(1));

        await _retention.RunOnceAsync(Options(keepRuns: 1, keepDays: 30), Now);

        foreach (var kept in new[] { running, paused, interrupted, pending, failedWithCheckpoint })
            Assert.IsTrue(await RunExistsAsync(kept), $"{kept.Status} run is kept");
        Assert.IsFalse(await RunExistsAsync(failedPlain));
        Assert.AreEqual(2, await CountAsync("RecordedDifferences", interrupted.Id));
        Assert.AreEqual(2, await CountAsync("RecordedDifferences", failedWithCheckpoint.Id));
    }

    [TestMethod]
    public async Task Deletes_AreDoneInBatches_AndRemoveEverything()
    {
        var old = await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(9), differences: 50);
        await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(1), differences: 3);

        // 50 rows with a batch of 7 (eight statements; the last is partial), and 14 audit rows with a batch of 7 (exact multiple).
        await using (var conn = await Factory.CreateOpenConnectionAsync())
        {
            for (int i = 0; i < 14; i++)
                await conn.ExecuteAsync("INSERT INTO ServiceAuditLogs (TimestampUtc, Level, Category, Message) VALUES (@t, 'INFO', 'c', 'm')", new { t = DbDateTime.ToDb(Now.AddDays(-200)) });
        }

        await using (var conn2 = await Factory.CreateOpenConnectionAsync())
            await conn2.ExecuteAsync("INSERT INTO ServiceAuditLogs (TimestampUtc, Level, Category, Message) VALUES (@t, 'INFO', 'c', 'recent')", new { t = DbDateTime.ToDb(Now.AddHours(-1)) });

        var result = await _retention.RunOnceAsync(Options(keepRuns: 1, auditDays: 30, batch: 7), Now);

        Assert.AreEqual(50, result.DifferencesDeleted);
        Assert.AreEqual(14, result.AuditLogsDeleted);
        Assert.IsFalse(await RunExistsAsync(old));
        Assert.AreEqual(3, await CountAsync("RecordedDifferences"));
    }

    [TestMethod]
    public async Task AuditLogs_ArePrunedByAge_AndKeptWhenTheSettingIsZero()
    {
        await using (var conn = await Factory.CreateOpenConnectionAsync())
        {
            await conn.ExecuteAsync("INSERT INTO ServiceAuditLogs (TimestampUtc, Level, Category, Message) VALUES (@t, 'INFO', 'c', 'old')", new { t = DbDateTime.ToDb(Now.AddDays(-100)) });
            await conn.ExecuteAsync("INSERT INTO ServiceAuditLogs (TimestampUtc, Level, Category, Message) VALUES (@t, 'INFO', 'c', 'new')", new { t = DbDateTime.ToDb(Now.AddDays(-1)) });
        }

        await _retention.RunOnceAsync(Options(auditDays: 0), Now);
        Assert.AreEqual(2, await CountAsync("ServiceAuditLogs"), "0 = keep for ever");

        var result = await _retention.RunOnceAsync(Options(auditDays: 90), Now);
        Assert.AreEqual(1, result.AuditLogsDeleted);
        Assert.AreEqual("new", await RawScalarAsync<string>("SELECT Message FROM ServiceAuditLogs"));
    }

    [TestMethod]
    public async Task AClockFarAheadOfTheData_SkipsTheAgeRules_ButNotTheCountRule()
    {
        var oldest = await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(3));
        var middle = await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(2));
        var newest = await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(1));
        await using (var conn = await Factory.CreateOpenConnectionAsync())
            await conn.ExecuteAsync("INSERT INTO ServiceAuditLogs (TimestampUtc, Level, Category, Message) VALUES (@t, 'INFO', 'c', 'm')", new { t = DbDateTime.ToDb(Now) });

        var wrongNow = Now.AddYears(3);
        var result = await _retention.RunOnceAsync(Options(keepRuns: 2, keepDays: 30, auditDays: 30), wrongNow);

        Assert.IsTrue(result.AgeRulesSkippedClockAhead);
        Assert.AreEqual(1, result.RunsDeleted, "only the count rule deleted something");
        Assert.IsFalse(await RunExistsAsync(oldest));
        Assert.IsTrue(await RunExistsAsync(middle));
        Assert.IsTrue(await RunExistsAsync(newest));
        Assert.AreEqual(0, result.AuditLogsDeleted);
        Assert.AreEqual(1, await CountAsync("ServiceAuditLogs"));
    }

    [TestMethod]
    public async Task AfterAPass_TheWalIsCheckpointed()
    {
        for (int i = 0; i < 4; i++) await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(10 - i), differences: 300);
        var result = await _retention.RunOnceAsync(Options(keepRuns: 1), Now);
        Assert.AreEqual("Truncated", result.Checkpoint);
        var walLength = File.Exists(DbPath + "-wal") ? new FileInfo(DbPath + "-wal").Length : 0;
        Assert.AreEqual(0, walLength, "TRUNCATE leaves an empty WAL file");
    }

    [TestMethod]
    public async Task IncrementalVacuum_ReturnsFreePagesToTheFile_OnANewDatabase()
    {
        await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(9), differences: 4000);
        await AddRunAsync("j", RunStatus.Completed, TimeSpan.FromDays(1));
        await RawScalarAsync<int>("PRAGMA wal_checkpoint(TRUNCATE)");
        long before = new FileInfo(DbPath).Length;

        var result = await _retention.RunOnceAsync(Options(keepRuns: 1), Now);

        long after = new FileInfo(DbPath).Length;
        Assert.IsTrue(result.PagesVacuumed > 0, "pages were freed");
        Assert.IsTrue(after < before, $"file shrank from {before} to {after}");
    }

    [TestMethod]
    public async Task AnExistingDatabaseWithoutAutoVacuum_IsConvertedOnce_WhenSmall_AndLeftAloneWhenLarge()
    {
        // A legacy file: created without the auto_vacuum pragma and already holding a table.
        var legacy = Path.Combine(Path.GetTempPath(), $"s3legacy_{Guid.NewGuid():N}.db");
        try
        {
            await using (var raw = new SqliteConnection($"Data Source={legacy};Mode=ReadWriteCreate;Pooling=False"))
            {
                await raw.OpenAsync();
                await raw.ExecuteAsync("CREATE TABLE Legacy (a TEXT);");
            }

            var factory = new SqliteConnectionFactory($"Data Source={legacy};Mode=ReadWriteCreate;Pooling=False");
            await new DatabaseMigrator(factory).MigrateAsync();
            var manager = new RetentionManager(factory);
            await using (var conn = await factory.CreateOpenConnectionAsync())
                Assert.AreEqual(0, await conn.ExecuteScalarAsync<int>("PRAGMA auto_vacuum"), "an existing file does not switch by itself");

            var tooSmallLimit = new RetentionOptions { AutoVacuumConvertMaxMegabytes = 0 };
            Assert.AreEqual(RetentionManager.VacuumConversion.SkippedTooLarge, await manager.EnsureIncrementalVacuumAsync(tooSmallLimit, legacy));

            var converted = await manager.EnsureIncrementalVacuumAsync(new RetentionOptions(), legacy);
            Assert.AreEqual(RetentionManager.VacuumConversion.Converted, converted);
            await using (var conn = await factory.CreateOpenConnectionAsync())
                Assert.AreEqual(2, await conn.ExecuteScalarAsync<int>("PRAGMA auto_vacuum"));
            Assert.AreEqual(RetentionManager.VacuumConversion.AlreadyIncremental, await manager.EnsureIncrementalVacuumAsync(new RetentionOptions(), legacy));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var suffix in new[] { "", "-shm", "-wal" })
            {
                try { File.Delete(legacy + suffix); } catch { }
            }
        }
    }

    [TestMethod]
    public async Task TheService_NeverThrowsFromAPass_EvenWhenTheDatabaseIsBroken()
    {
        var broken = new SqliteConnectionFactory($"Data Source={Path.Combine(Path.GetTempPath(), $"nodir_{Guid.NewGuid():N}", "x.db")};Mode=ReadWrite;");
        var service = new RetentionService(new RetentionManager(broken), new RetentionOptions(), TimeProvider.System, NullLogger<RetentionService>.Instance);
        await service.RunPassAsync(CancellationToken.None); // must not throw
        Assert.IsFalse(service.HasCompleted);

        var good = new RetentionService(_retention, Options(keepRuns: 5), TimeProvider.System, NullLogger<RetentionService>.Instance);
        await good.RunPassAsync(CancellationToken.None);
        Assert.IsTrue(good.HasCompleted);
        Assert.IsNotNull(good.LastResult);
    }

    // ---------- the difference cap ----------

    private static List<S3ObjectSummary> Objects(int count) =>
        Enumerable.Range(0, count).Select(i => new S3ObjectSummary { Key = $"obj/{i:D5}", Size = 1, ETag = "e", LastModified = DateTime.UtcNow.AddDays(-1) }).ToList();

    [TestMethod]
    public async Task DifferenceCap_StopsInsertingRows_ButTheCountersStayExact_AndTheRunIsFlagged()
    {
        var job = new ComparisonJob { Name = "cap", ReplicationGracePeriodMinutes = 0, DetectSequenceGaps = false };
        var run = new ScanRun { JobId = job.Id, JobName = job.Name };
        await RunRepo.CreateAsync(run);

        // 120 objects only in A: 120 "missing in B" differences.
        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(Objects(120)), streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()),
            options: new ScanOptions { MaxRecordedDifferencesPerRun = 40 });
        var done = await engine.RunAsync();

        Assert.AreEqual(RunStatus.Completed, done.Status);
        Assert.AreEqual(120, done.TotalDifferences);
        Assert.AreEqual(120, done.MissingInBucketBCount);
        Assert.AreEqual(120, done.FilteredDifferencesCount);
        Assert.AreEqual(40, await DiffRepo.GetDifferenceCountAsync(run.Id));
        Assert.IsTrue(done.DifferencesTruncated);
        StringAssert.Contains(done.ScanWindowNotes, "truncated");

        var stored = await RunRepo.GetByIdAsync(run.Id);
        Assert.IsTrue(stored!.DifferencesTruncated, "the flag is persisted");
        Assert.AreEqual(120, stored.MissingInBucketBCount);
        StringAssert.Contains(stored.ScanWindowNotes, "truncated");

        // Exports say so.
        using var csv = new MemoryStream();
        await ReportExporter.ExportToCsvStreamAsync(csv, stored, DiffRepo.StreamAllForRunAsync(run.Id));
        var text = System.Text.Encoding.UTF8.GetString(csv.ToArray());
        StringAssert.Contains(text, "TRUNCATED");
        using var json = new MemoryStream();
        await ReportExporter.ExportToJsonStreamAsync(json, stored, DiffRepo.StreamAllForRunAsync(run.Id));
        StringAssert.Contains(System.Text.Encoding.UTF8.GetString(json.ToArray()), "\"differencesTruncated\":true");
    }

    [TestMethod]
    public async Task DifferenceCap_BelowTheLimit_DoesNotFlagTheRun_AndZeroMeansUnlimited()
    {
        foreach (var cap in new long[] { 100, 0 })
        {
            var job = new ComparisonJob { Name = "nocap", ReplicationGracePeriodMinutes = 0, DetectSequenceGaps = false };
            var run = new ScanRun { JobId = job.Id, JobName = job.Name };
            await RunRepo.CreateAsync(run);
            var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
                streamA: new MockS3ObjectStream(Objects(60)), streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()),
                options: new ScanOptions { MaxRecordedDifferencesPerRun = cap });
            var done = await engine.RunAsync();
            Assert.IsFalse(done.DifferencesTruncated);
            Assert.AreEqual(60, await DiffRepo.GetDifferenceCountAsync(run.Id));
        }
    }

    [TestMethod]
    public async Task DifferenceCap_AppliesAcrossAResume_SoTheRowsStayAtTheCap()
    {
        var job = new ComparisonJob { Name = "capresume", ReplicationGracePeriodMinutes = 0, DetectSequenceGaps = false };
        var run = new ScanRun { JobId = job.Id, JobName = job.Name };
        await RunRepo.CreateAsync(run);
        var items = Objects(100);
        ServiceComparisonEngine? engine1 = null;
        using var cts = new CancellationTokenSource();
        engine1 = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo, AuditRepo,
            streamA: new MockS3ObjectStream(items, onMoveNext: n => { if (n == 60) { engine1!.MarkServiceShutdown(); cts.Cancel(); } }),
            streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()),
            options: new ScanOptions { MaxRecordedDifferencesPerRun = 30 });
        await Assert.ThrowsAsync<OperationCanceledException>(() => engine1.RunAsync(cts.Token));

        var cp = await RunRepo.GetCheckpointAsync(run.Id);
        Assert.IsNotNull(cp);
        var reopened = await RunRepo.ReopenForResumeAsync(run.Id);
        var engine2 = new ServiceComparisonEngine(job, reopened!, RunRepo, DiffRepo, AuditRepo, cp,
            streamA: new MockS3ObjectStream(items, startAfter: cp.LastEvaluatedKey), streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()),
            options: new ScanOptions { MaxRecordedDifferencesPerRun = 30 });
        var done = await engine2.RunAsync();

        Assert.AreEqual(100, done.TotalDifferences);
        Assert.AreEqual(30, await DiffRepo.GetDifferenceCountAsync(run.Id));
        Assert.IsTrue(done.DifferencesTruncated);
    }
}
