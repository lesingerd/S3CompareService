using System.Text.Json;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

[TestClass]
public class StorageHardeningTests : TestDbBase
{
    // ---------- A. DateTime kind handling ----------

    [TestMethod]
    public async Task Job_RoundTrip_ReturnsUtcKindAndEqualValues()
    {
        var job = new ComparisonJob { Name = "utc job" };
        job.CreatedAtUtc = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc);
        await JobRepo.SaveAsync(job);
        await JobRepo.UpdateRunScheduleStateAsync(job.Id, new DateTime(2026, 3, 4, 6, 0, 0, DateTimeKind.Utc), new DateTime(2026, 3, 4, 7, 0, 0, DateTimeKind.Utc), RunStatus.Completed);

        var loaded = await JobRepo.GetByIdAsync(job.Id);
        Assert.IsNotNull(loaded);
        Assert.AreEqual(DateTimeKind.Utc, loaded.CreatedAtUtc.Kind);
        Assert.AreEqual(DateTimeKind.Utc, loaded.UpdatedAtUtc.Kind);
        Assert.AreEqual(DateTimeKind.Utc, loaded.LastRunTimeUtc!.Value.Kind);
        Assert.AreEqual(DateTimeKind.Utc, loaded.NextRunTimeUtc!.Value.Kind);
        Assert.AreEqual(job.CreatedAtUtc, loaded.CreatedAtUtc);
        Assert.AreEqual(new DateTime(2026, 3, 4, 6, 0, 0, DateTimeKind.Utc), loaded.LastRunTimeUtc);

        // Cronos requires a UTC DateTime; this used to throw because the value came back as Local.
        var cron = Cronos.CronExpression.Parse("*/5 * * * *");
        Assert.IsNotNull(cron.GetNextOccurrence(loaded.LastRunTimeUtc!.Value, TimeZoneInfo.Utc));
    }

    [TestMethod]
    public async Task LocalKindValues_AreStoredAsUtc()
    {
        var job = new ComparisonJob { Name = "local job" };
        var local = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Local);
        job.CreatedAtUtc = local;
        await JobRepo.SaveAsync(job);

        var loaded = await JobRepo.GetByIdAsync(job.Id);
        Assert.AreEqual(DateTimeKind.Utc, loaded!.CreatedAtUtc.Kind);
        Assert.AreEqual(local.ToUniversalTime(), loaded.CreatedAtUtc);
    }

    [TestMethod]
    public async Task ScanRunDifferenceAuditAndCheckpoint_DatesComeBackUtc()
    {
        var run = new ScanRun { JobId = "j", JobName = "n", StartTimeUtc = DateTime.UtcNow.AddMinutes(-3), EndTimeUtc = DateTime.UtcNow, GracePeriodCutoffUtc = DateTime.UtcNow.AddMinutes(-18) };
        await RunRepo.CreateAsync(run);
        await DiffRepo.InsertBatchAsync(new[]
        {
            new RecordedDifference { ScanRunId = run.Id, Key = "k", Type = DifferenceType.SizeMismatch, BucketALastModified = DateTime.UtcNow, GracePeriodCutoffUtc = DateTime.UtcNow }
        });
        await AuditRepo.LogAsync("INFO", "t", "m");
        await RunRepo.SaveCheckpointAsync(new ScanCheckpoint { ScanRunId = run.Id, JobId = "j", LastEvaluatedKey = "k" });

        var r = await RunRepo.GetByIdAsync(run.Id);
        Assert.AreEqual(DateTimeKind.Utc, r!.StartTimeUtc.Kind);
        Assert.AreEqual(DateTimeKind.Utc, r.EndTimeUtc!.Value.Kind);
        Assert.AreEqual(DateTimeKind.Utc, r.GracePeriodCutoffUtc!.Value.Kind);
        Assert.AreEqual(run.StartTimeUtc, r.StartTimeUtc);

        var d = (await DiffRepo.GetDifferencesAsync(run.Id)).Single();
        Assert.AreEqual(DateTimeKind.Utc, d.DetectedAtUtc.Kind);
        Assert.AreEqual(DateTimeKind.Utc, d.BucketALastModified!.Value.Kind);
        Assert.AreEqual(DateTimeKind.Utc, d.GracePeriodCutoffUtc!.Value.Kind);

        var log = (await AuditRepo.GetRecentLogsAsync(5)).Single();
        Assert.AreEqual(DateTimeKind.Utc, log.TimestampUtc.Kind);

        var cp = await RunRepo.GetCheckpointAsync(run.Id);
        Assert.AreEqual(DateTimeKind.Utc, cp!.LastUpdatedAtUtc.Kind);
    }

    // ---------- 14. Server-owned state preserved, targeted updates ----------

    [TestMethod]
    public async Task SaveAsync_Update_PreservesServerOwnedState()
    {
        var job = new ComparisonJob { Name = "orig" };
        job.AlertConfig.IsEnabled = true;
        await JobRepo.SaveAsync(job);

        var lastRun = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var next = lastRun.AddHours(1);
        var alertAt = new DateTime(2026, 1, 1, 10, 5, 0, DateTimeKind.Utc);
        var digestAt = new DateTime(2026, 1, 1, 8, 0, 0, DateTimeKind.Utc);
        await JobRepo.UpdateRunScheduleStateAsync(job.Id, lastRun, next, RunStatus.Completed);
        await JobRepo.UpdateAlertTimestampsAsync(job.Id, alertAt, digestAt);

        // A stale UI copy that knows nothing of the server state saves a rename.
        var stale = new ComparisonJob { Id = job.Id, Name = "renamed", CreatedAtUtc = job.CreatedAtUtc };
        stale.AlertConfig.IsEnabled = true;
        await JobRepo.SaveAsync(stale);

        var loaded = await JobRepo.GetByIdAsync(job.Id);
        Assert.AreEqual("renamed", loaded!.Name);
        Assert.AreEqual(lastRun, loaded.LastRunTimeUtc);
        Assert.AreEqual(next, loaded.NextRunTimeUtc);
        Assert.AreEqual(RunStatus.Completed, loaded.LastRunStatus);
        Assert.AreEqual(alertAt, loaded.AlertConfig.LastAlertSentUtc);
        Assert.AreEqual(digestAt, loaded.AlertConfig.LastDailyDigestSentUtc);
    }

    [TestMethod]
    public async Task TargetedUpdates_ChangeOnlyTheirFields()
    {
        var job = new ComparisonJob { Name = "targeted" };
        job.AlertConfig.IsEnabled = true;
        job.AlertConfig.SmtpHost = "smtp.example.com";
        job.AlertConfig.SmtpPassword = "pw-secret";
        await JobRepo.SaveAsync(job);

        var t1 = new DateTime(2026, 2, 2, 2, 2, 2, DateTimeKind.Utc);
        await JobRepo.UpdateNextRunTimeAsync(job.Id, t1);
        await JobRepo.UpdateAlertTimestampsAsync(job.Id, lastAlertSentUtc: t1, lastDailyDigestSentUtc: null);
        await JobRepo.UpdateAlertTimestampsAsync(job.Id, lastAlertSentUtc: null, lastDailyDigestSentUtc: t1.AddHours(1));

        var loaded = await JobRepo.GetByIdAsync(job.Id);
        Assert.AreEqual(t1, loaded!.NextRunTimeUtc);
        Assert.AreEqual(t1, loaded.AlertConfig.LastAlertSentUtc);
        Assert.AreEqual(t1.AddHours(1), loaded.AlertConfig.LastDailyDigestSentUtc);
        Assert.AreEqual("smtp.example.com", loaded.AlertConfig.SmtpHost);
        Assert.AreEqual("pw-secret", loaded.AlertConfig.SmtpPassword); // still decrypts after raw JSON round trip
    }

    // ---------- 16. Secrets ----------

    [TestMethod]
    public async Task Secrets_AreEncryptedAtRest_AndTransparentToCallers()
    {
        var job = new ComparisonJob { Name = "secrets" };
        job.BucketA.AccessKey = "AKIA-PLAIN-A";
        job.BucketA.SecretKey = "SECRET-PLAIN-A";
        job.BucketB.SecretKey = "SECRET-PLAIN-B";
        job.AlertConfig.SmtpPassword = "SMTP-PLAIN";
        job.AlertConfig.AzureWebhookUrl = "https://hooks.example.com/PLAIN";
        job.AlertConfig.AzureAppInsightsConnectionString = "InstrumentationKey=PLAIN";
        await JobRepo.SaveAsync(job);

        string raw = (await RawScalarAsync<string>("SELECT BucketA_Json || BucketB_Json || AlertConfig_Json FROM Jobs WHERE Id = @Id", new { job.Id }))!;
        foreach (var s in new[] { "AKIA-PLAIN-A", "SECRET-PLAIN-A", "SECRET-PLAIN-B", "SMTP-PLAIN", "hooks.example.com", "InstrumentationKey=PLAIN" })
        {
            Assert.IsFalse(raw.Contains(s), $"'{s}' stored in plaintext");
        }
        if (OperatingSystem.IsWindows())
        {
            Assert.IsTrue(raw.Contains(SecretProtector.Prefix));
        }

        var loaded = (await JobRepo.GetByIdAsync(job.Id))!;
        Assert.AreEqual("AKIA-PLAIN-A", loaded.BucketA.AccessKey);
        Assert.AreEqual("SECRET-PLAIN-A", loaded.BucketA.SecretKey);
        Assert.AreEqual("SECRET-PLAIN-B", loaded.BucketB.SecretKey);
        Assert.AreEqual("SMTP-PLAIN", loaded.AlertConfig.SmtpPassword);
        Assert.AreEqual("https://hooks.example.com/PLAIN", loaded.AlertConfig.AzureWebhookUrl);
        Assert.AreEqual("InstrumentationKey=PLAIN", loaded.AlertConfig.AzureAppInsightsConnectionString);
        // the caller's in-memory object is never mutated into ciphertext
        Assert.AreEqual("SECRET-PLAIN-A", job.BucketA.SecretKey);
    }

    [TestMethod]
    public async Task LegacyPlaintextSecrets_StillLoad_AndGetEncryptedOnNextSave()
    {
        var job = new ComparisonJob { Name = "legacy" };
        job.BucketA.SecretKey = "x";
        await JobRepo.SaveAsync(job);

        // Simulate a pre-encryption database row.
        string legacyJson = JsonSerializer.Serialize(new S3BucketConfig { BucketName = "b", AccessKey = "LEGACY-AK", SecretKey = "LEGACY-SK" });
        await using (var conn = await Factory.CreateOpenConnectionAsync())
        {
            await Dapper.SqlMapper.ExecuteAsync(conn, "UPDATE Jobs SET BucketA_Json = @J WHERE Id = @Id", new { J = legacyJson, job.Id });
        }

        var loaded = (await JobRepo.GetByIdAsync(job.Id))!;
        Assert.AreEqual("LEGACY-SK", loaded.BucketA.SecretKey);

        await JobRepo.SaveAsync(loaded);
        string raw = (await RawScalarAsync<string>("SELECT BucketA_Json FROM Jobs WHERE Id = @Id", new { job.Id }))!;
        if (OperatingSystem.IsWindows())
        {
            Assert.IsFalse(raw.Contains("LEGACY-SK"));
            Assert.IsTrue(raw.Contains(SecretProtector.Prefix));
        }
        Assert.AreEqual("LEGACY-SK", (await JobRepo.GetByIdAsync(job.Id))!.BucketA.SecretKey);
    }

    [TestMethod]
    public async Task SecretMask_MaskedCopyAndMergeKeepStoredSecrets()
    {
        var job = new ComparisonJob { Name = "mask" };
        job.BucketA.AccessKey = "AK";
        job.BucketA.SecretKey = "SK";
        job.AlertConfig.SmtpPassword = "PW";
        job.AlertConfig.AzureWebhookUrl = "https://hook";
        await JobRepo.SaveAsync(job);

        var masked = SecretMask.MaskedCopy((await JobRepo.GetByIdAsync(job.Id))!);
        Assert.AreEqual(SecretMask.Mask, masked.BucketA.SecretKey);
        Assert.AreEqual(SecretMask.Mask, masked.AlertConfig.SmtpPassword);
        Assert.AreEqual(string.Empty, masked.BucketB.SecretKey, "unset secrets stay empty so clients can tell");
        Assert.AreEqual("SK", job.BucketA.SecretKey, "original untouched");

        // Client edits only the name and posts the masked secrets back; one secret is replaced.
        masked.Name = "mask-renamed";
        masked.AlertConfig.SmtpPassword = "NEW-PW";
        await JobRepo.SaveAsync(masked);

        var loaded = (await JobRepo.GetByIdAsync(job.Id))!;
        Assert.AreEqual("mask-renamed", loaded.Name);
        Assert.AreEqual("AK", loaded.BucketA.AccessKey);
        Assert.AreEqual("SK", loaded.BucketA.SecretKey);
        Assert.AreEqual("NEW-PW", loaded.AlertConfig.SmtpPassword);
        Assert.AreEqual("https://hook", loaded.AlertConfig.AzureWebhookUrl);
    }

    // ---------- 24. Migrations ----------

    [TestMethod]
    public async Task Migrator_IsVersioned_Idempotent_AndSafeUnderConcurrency()
    {
        Assert.AreEqual(DatabaseMigrator.LatestVersion, await RawScalarAsync<int>("PRAGMA user_version;"));

        // Pre-versioning database (user_version 0 with all tables present) is accepted.
        await using (var conn = await Factory.CreateOpenConnectionAsync())
        {
            await Dapper.SqlMapper.ExecuteAsync(conn, "PRAGMA user_version = 0;");
        }
        await new DatabaseMigrator(Factory).MigrateAsync();
        Assert.AreEqual(DatabaseMigrator.LatestVersion, await RawScalarAsync<int>("PRAGMA user_version;"));

        // Several processes (simulated) migrating a brand new database at the same time.
        string path = Path.Combine(Path.GetTempPath(), $"s3mig_{Guid.NewGuid():N}.db");
        try
        {
            var tasks = Enumerable.Range(0, 6)
                .Select(_ => Task.Run(() => new DatabaseMigrator(new SqliteConnectionFactory($"Data Source={path};Mode=ReadWriteCreate;")).MigrateAsync()))
                .ToArray();
            await Task.WhenAll(tasks);

            await using var conn = await new SqliteConnectionFactory($"Data Source={path};Mode=ReadWriteCreate;").CreateOpenConnectionAsync();
            Assert.AreEqual(DatabaseMigrator.LatestVersion, await Dapper.SqlMapper.ExecuteScalarAsync<int>(conn, "PRAGMA user_version;"));
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            foreach (var suffix in new[] { "", "-shm", "-wal" }) { try { File.Delete(path + suffix); } catch { } }
        }
    }

    // ---------- 4 / 5. Unfinished-run query, reopen, supersede ----------

    [TestMethod]
    public async Task GetUnfinishedRuns_IncludesRunningPausedInterrupted_ExcludesTerminalAndSuperseded()
    {
        var ids = new Dictionary<RunStatus, string>();
        foreach (var status in Enum.GetValues<RunStatus>())
        {
            var run = new ScanRun { JobId = "j", JobName = "n", Status = status };
            await RunRepo.CreateAsync(run);
            ids[status] = run.Id;
        }

        var unfinished = (await RunRepo.GetUnfinishedRunsAsync()).Select(r => r.Id).ToHashSet();
        CollectionAssert.AreEquivalent(
            new[] { ids[RunStatus.Running], ids[RunStatus.Paused], ids[RunStatus.Interrupted] },
            unfinished.ToArray());

        // Once a newer run has taken over the interrupted run, it is no longer returned.
        await RunRepo.PrepareResumeAsync("new-run", ids[RunStatus.Interrupted], "");
        unfinished = (await RunRepo.GetUnfinishedRunsAsync()).Select(r => r.Id).ToHashSet();
        Assert.IsFalse(unfinished.Contains(ids[RunStatus.Interrupted]));
        Assert.AreEqual("new-run", (await RunRepo.GetByIdAsync(ids[RunStatus.Interrupted]))!.SupersededByRunId);
    }

    [TestMethod]
    public async Task ReopenForResume_FlipsInterruptedRunBackToRunning_OnlyWhenResumable()
    {
        var run = new ScanRun { JobId = "j", JobName = "n", Status = RunStatus.Interrupted, EndTimeUtc = DateTime.UtcNow, ErrorMessage = "boom", LastEvaluatedKey = "k42" };
        await RunRepo.CreateAsync(run);

        var reopened = await RunRepo.ReopenForResumeAsync(run.Id);
        Assert.IsNotNull(reopened);
        Assert.AreEqual(RunStatus.Running, reopened.Status);
        Assert.IsNull(reopened.EndTimeUtc);
        Assert.IsNull(reopened.ErrorMessage);
        Assert.IsTrue(reopened.IsResumedFromCheckpoint);
        Assert.AreEqual("k42", reopened.ResumedFromKey);
        Assert.AreEqual(1, reopened.ResumeCount);

        var done = new ScanRun { JobId = "j", JobName = "n", Status = RunStatus.Completed };
        await RunRepo.CreateAsync(done);
        Assert.IsNull(await RunRepo.ReopenForResumeAsync(done.Id));
        Assert.IsNull(await RunRepo.ReopenForResumeAsync("missing"));
    }

    [TestMethod]
    public async Task PrepareResume_RemovesDifferencesAfterCheckpointKey_UsingUtf8Order()
    {
        var run = new ScanRun { JobId = "j", JobName = "n" };
        await RunRepo.CreateAsync(run);
        string emoji = "\U0001F600";
        await DiffRepo.InsertBatchAsync(new[] { "a", "b", "！", emoji, "z" }.Select(k =>
            new RecordedDifference { ScanRunId = run.Id, Key = k, Type = DifferenceType.MissingInBucketB }));

        int removed = await RunRepo.PrepareResumeAsync(run.Id, run.Id, "！");

        // UTF-8 order: a < b < z < U+FF01 < emoji  => only the emoji is after the checkpoint (UTF-16 order would also drop z).
        Assert.AreEqual(1, removed);
        var left = (await DiffRepo.GetAllForRunAsync(run.Id)).Select(d => d.Key).ToList();
        CollectionAssert.AreEqual(new[] { "a", "b", "！", "z" }, left);
    }

    // ---------- 6. Run metadata persisted ----------

    [TestMethod]
    public async Task RunMetadata_IsPersistedByUpdateProgressAndComplete()
    {
        var run = new ScanRun { JobId = "j", JobName = "n" };
        await RunRepo.CreateAsync(run);

        run.StartTimeUtc = new DateTime(2026, 5, 5, 5, 5, 5, DateTimeKind.Utc);
        run.ReplicationGracePeriodMinutes = 42;
        run.GracePeriodCutoffUtc = run.StartTimeUtc.AddMinutes(-42);
        run.IsScanWindowClipped = true;
        run.ScanWindowNotes = "clipped note";
        await RunRepo.UpdateProgressAsync(run);

        var r = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(run.StartTimeUtc, r.StartTimeUtc);
        Assert.AreEqual(42, r.ReplicationGracePeriodMinutes);
        Assert.AreEqual(run.GracePeriodCutoffUtc, r.GracePeriodCutoffUtc);
        Assert.IsTrue(r.IsScanWindowClipped);
        Assert.AreEqual("clipped note", r.ScanWindowNotes);

        run.ScanWindowNotes = "final note";
        run.Status = RunStatus.Completed;
        run.EndTimeUtc = DateTime.UtcNow;
        await RunRepo.CompleteAsync(run);
        Assert.AreEqual("final note", (await RunRepo.GetByIdAsync(run.Id))!.ScanWindowNotes);
    }

    [TestMethod]
    public async Task Engine_PersistsGraceWindowMetadata()
    {
        var job = new ComparisonJob { Name = "meta", ReplicationGracePeriodMinutes = 15 };
        var run = new ScanRun { JobId = job.Id, JobName = job.Name, StartTimeUtc = DateTime.UtcNow.AddDays(-1) };
        await RunRepo.CreateAsync(run);

        var engine = new ServiceComparisonEngine(job, run, RunRepo, DiffRepo,
            streamA: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()), streamB: new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()));
        await engine.RunAsync();

        var r = (await RunRepo.GetByIdAsync(run.Id))!;
        Assert.AreEqual(15, r.ReplicationGracePeriodMinutes);
        Assert.IsNotNull(r.GracePeriodCutoffUtc);
        Assert.IsTrue(r.IsScanWindowClipped);
        Assert.IsTrue(r.ScanWindowNotes.Contains("grace window"));
        Assert.IsTrue((DateTime.UtcNow - r.StartTimeUtc).TotalMinutes < 5, "engine start time was persisted, not the stale constructor value");
        Assert.AreEqual(r.StartTimeUtc.AddMinutes(-15), r.GracePeriodCutoffUtc);
    }

    // ---------- 26. LIKE escaping, 17 streaming ----------

    [TestMethod]
    public async Task KeySearch_TreatsWildcardsLiterally()
    {
        var run = new ScanRun { JobId = "j", JobName = "n" };
        await RunRepo.CreateAsync(run);
        await DiffRepo.InsertBatchAsync(new[] { "a_b", "axb", "100%", "1000", @"c\d", "cxd" }.Select(k =>
            new RecordedDifference { ScanRunId = run.Id, Key = k, Type = DifferenceType.MissingInBucketB }));

        Assert.AreEqual(1, await DiffRepo.GetDifferenceCountAsync(run.Id, searchKey: "a_b"));
        Assert.AreEqual(1, await DiffRepo.GetDifferenceCountAsync(run.Id, searchKey: "100%"));
        Assert.AreEqual(1, await DiffRepo.GetDifferenceCountAsync(run.Id, searchKey: @"c\d"));
        Assert.AreEqual(1, await DiffRepo.GetDifferenceCountAsync(run.Id, searchKey: "%"));
        Assert.AreEqual(1, await DiffRepo.GetDifferenceCountAsync(run.Id, searchKey: "_b")); // only a_b, not axb
    }

    [TestMethod]
    public async Task StreamAllForRun_YieldsEverythingInOrder_AndExportsStream()
    {
        var run = new ScanRun { JobId = "j", JobName = "stream-job", Status = RunStatus.Completed };
        await RunRepo.CreateAsync(run);
        var diffs = Enumerable.Range(0, 1200).Select(i => new RecordedDifference
        {
            ScanRunId = run.Id,
            Key = $"k{i:0000}",
            Type = DifferenceType.MissingInBucketB,
            Details = i == 3 ? "=HYPERLINK(\"http://evil\")" : "d",
            IsWithinGracePeriod = i % 2 == 0
        }).ToList();
        await DiffRepo.InsertBatchAsync(diffs);

        int n = 0;
        string? prev = null;
        await foreach (var d in DiffRepo.StreamAllForRunAsync(run.Id))
        {
            if (prev != null) Assert.IsTrue(string.CompareOrdinal(prev, d.Key) < 0);
            prev = d.Key;
            n++;
        }
        Assert.AreEqual(1200, n);

        // JSON stream is valid JSON with the right count and honours the in-flight filter.
        using var jsonMs = new MemoryStream();
        await ReportExporter.ExportToJsonStreamAsync(jsonMs, run, DiffRepo.StreamAllForRunAsync(run.Id, false), null, includeGracePeriodItems: false);
        jsonMs.Position = 0;
        using var doc = JsonDocument.Parse(jsonMs);
        Assert.AreEqual(600, doc.RootElement.GetProperty("differences").GetArrayLength());
        Assert.AreEqual(600, doc.RootElement.GetProperty("totalExportedDifferences").GetInt64());
        Assert.AreEqual(run.Id, doc.RootElement.GetProperty("run").GetProperty("id").GetString());

        // CSV stream.
        using var csvMs = new MemoryStream();
        await ReportExporter.ExportToCsvStreamAsync(csvMs, run, DiffRepo.StreamAllForRunAsync(run.Id));
        string csv = System.Text.Encoding.UTF8.GetString(csvMs.ToArray());
        Assert.AreEqual(1200, csv.Split('\n').Count(l => l.Contains(",MissingInBucketB,")));
        Assert.IsTrue(csv.Contains("\"'=HYPERLINK("), "formula injection must be neutralised");
        Assert.IsFalse(csv.Contains(",\"=HYPERLINK("));
    }

    [TestMethod]
    public void Csv_Escape_NeutralisesFormulaPrefixes()
    {
        foreach (var bad in new[] { "=1+1", "+1", "-1", "@SUM(A1)", "\tx", "\rx" })
        {
            Assert.IsTrue(ReportExporter.EscapeCsv(bad).StartsWith("\"'"), bad);
        }
        Assert.AreEqual("\"normal\"", ReportExporter.EscapeCsv("normal"));
        Assert.AreEqual("\"a\"\"b\"", ReportExporter.EscapeCsv("a\"b"));
        Assert.AreEqual("\"\"", ReportExporter.EscapeCsv(null));
    }

    // ---------- 7. UTF-8 comparer ----------

    [TestMethod]
    public void Utf8Comparer_OrdersByCodePoint_NotUtf16()
    {
        string emoji = "\U0001F600"; // surrogate pair D83D DE00, UTF-8 F0 9F 98 80
        string fullwidth = "！";  // UTF-8 EF BC 81
        Assert.IsTrue(string.CompareOrdinal(emoji, fullwidth) < 0, "UTF-16 ordinal disagrees with S3 (that is the bug)");
        Assert.IsTrue(Utf8OrdinalComparer.CompareStatic(emoji, fullwidth) > 0);
        Assert.IsTrue(Utf8OrdinalComparer.CompareStatic(fullwidth, emoji) < 0);
        Assert.AreEqual(0, Utf8OrdinalComparer.CompareStatic(emoji, "\U0001F600"));
        Assert.IsTrue(Utf8OrdinalComparer.CompareStatic("a", "ab") < 0);

        // Cross-check against real UTF-8 byte ordering on a mixed set.
        var keys = new[] { "z", "\U0001F600", "！", "a", "é", "", "\U00010000", "퟿", "" };
        var byBytes = keys.OrderBy(k => k, Comparer<string>.Create((x, y) =>
            System.Text.Encoding.UTF8.GetBytes(x).AsSpan().SequenceCompareTo(System.Text.Encoding.UTF8.GetBytes(y)))).ToArray();
        var byComparer = keys.OrderBy(k => k, Utf8OrdinalComparer.Instance).ToArray();
        CollectionAssert.AreEqual(byBytes, byComparer);
    }
}
