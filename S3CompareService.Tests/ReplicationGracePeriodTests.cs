using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

[TestClass]
public class ReplicationGracePeriodTests
{
    private string _testDbPath = string.Empty;
    private SqliteConnectionFactory _factory = null!;
    private ScanRunRepository _runRepo = null!;
    private DifferenceRepository _diffRepo = null!;

    [TestInitialize]
    public async Task Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"s3grace_{Guid.NewGuid():N}.db");
        _factory = new SqliteConnectionFactory($"Data Source={_testDbPath};Mode=ReadWriteCreate;");
        var migrator = new DatabaseMigrator(_factory);
        await migrator.MigrateAsync();

        _runRepo = new ScanRunRepository(_factory);
        _diffRepo = new DifferenceRepository(_factory);
    }

    [TestCleanup]
    public void Cleanup()
    {
        try
        {
            if (File.Exists(_testDbPath)) File.Delete(_testDbPath);
            string shm = _testDbPath + "-shm";
            string wal = _testDbPath + "-wal";
            if (File.Exists(shm)) File.Delete(shm);
            if (File.Exists(wal)) File.Delete(wal);
        }
        catch { }
    }

    [TestMethod]
    public async Task GracePeriod_RecentFile_TaggedAsInFlight_WhileOldFile_TaggedAsConfirmedDisparity()
    {
        var now = DateTime.UtcNow;

        // item1: Modified 5 minutes ago (within 15m grace period -> IN-FLIGHT REPLICATION)
        // item2: Modified 2 hours ago (outside 15m grace period -> CONFIRMED DISPARITY)
        // item3: Identical matching item
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "recent_replication.dat", Size = 1000, ETag = "tag1", LastModified = now.AddMinutes(-5) },
            new() { Key = "old_stale_missing.dat", Size = 2000, ETag = "tag2", LastModified = now.AddHours(-2) },
            new() { Key = "matching_doc.txt", Size = 500, ETag = "tag3", LastModified = now.AddHours(-1) }
        };

        // Bucket B only has the matching doc
        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "matching_doc.txt", Size = 500, ETag = "tag3", LastModified = now.AddHours(-1) }
        };

        var job = new ComparisonJob
        {
            Name = "Replication Grace Period Test",
            ReplicationGracePeriodMinutes = 15,
            Mode = ComparisonMode.ListingOnly
        };

        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = job.Id, JobName = job.Name };
        await _runRepo.CreateAsync(run);

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new ServiceComparisonEngine(job, run, _runRepo, _diffRepo, streamA: streamA, streamB: streamB);
        var completedRun = await engine.RunAsync();

        // 1. Verify ScanRun window tracking
        Assert.IsTrue(completedRun.IsScanWindowClipped);
        Assert.IsNotNull(completedRun.GracePeriodCutoffUtc);
        Assert.AreEqual(15, completedRun.ReplicationGracePeriodMinutes);
        Assert.IsTrue(completedRun.ScanWindowNotes.Contains("Replication lag grace window active"));

        // 2. Summary stats verification
        Assert.AreEqual(3, completedRun.ObjectsCompared);
        Assert.AreEqual(1, completedRun.MatchedCount);
        Assert.AreEqual(2, completedRun.TotalDifferences, "Total raw differences should be 2 for raw completeness");
        Assert.AreEqual(1, completedRun.FilteredDifferencesCount, "Confirmed disparities should only be 1 (the old file)");
        Assert.AreEqual(1, completedRun.InFlightDifferencesCount, "In-flight replication should be 1 (the recent file)");

        // 3. Inspect raw stored differences in DB
        var allDiffs = await _diffRepo.GetAllForRunAsync(completedRun.Id, withinGracePeriodFilter: null);
        Assert.AreEqual(2, allDiffs.Count, "All differences must be stored in database for completeness");

        var recentDiff = allDiffs.First(d => d.Key == "recent_replication.dat");
        Assert.IsTrue(recentDiff.IsWithinGracePeriod);
        Assert.AreEqual("IN_FLIGHT_REPLICATION", recentDiff.ScanWindowTag);

        var oldDiff = allDiffs.First(d => d.Key == "old_stale_missing.dat");
        Assert.IsFalse(oldDiff.IsWithinGracePeriod);
        Assert.AreEqual("CONFIRMED_DISPARITY", oldDiff.ScanWindowTag);

        // 4. Test querying with grace period filtering (clean report view)
        var confirmedOnly = await _diffRepo.GetAllForRunAsync(completedRun.Id, withinGracePeriodFilter: false);
        Assert.AreEqual(1, confirmedOnly.Count);
        Assert.AreEqual("old_stale_missing.dat", confirmedOnly[0].Key);

        var inFlightOnly = await _diffRepo.GetAllForRunAsync(completedRun.Id, withinGracePeriodFilter: true);
        Assert.AreEqual(1, inFlightOnly.Count);
        Assert.AreEqual("recent_replication.dat", inFlightOnly[0].Key);
    }

    [TestMethod]
    public async Task GracePeriod_InFlightDifferences_DoNotIncrementConfirmedPerTypeCounters_OrTriggerPerTypeAlerts()
    {
        var now = DateTime.UtcNow;

        // All 3 differences are modified within the 15-minute replication grace window
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "inflight_1.dat", Size = 1000, ETag = "tag1", LastModified = now.AddMinutes(-2) },
            new() { Key = "inflight_2.dat", Size = 2000, ETag = "tag2", LastModified = now.AddMinutes(-4) },
            new() { Key = "inflight_size.dat", Size = 3000, ETag = "tag3", LastModified = now.AddMinutes(-1) }
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "inflight_size.dat", Size = 1500, ETag = "tag3_old", LastModified = now.AddMinutes(-3) }
        };

        var job = new ComparisonJob
        {
            Name = "In-Flight Shield Alert Test",
            ReplicationGracePeriodMinutes = 15,
            Mode = ComparisonMode.ListingOnly,
            AlertConfig = new JobAlertConfig
            {
                IsEnabled = true,
                AlertOnDisparities = true,
                MaxDisparitiesThreshold = 0,
                MaxMissingInBThreshold = 0,
                MaxSizeMismatchThreshold = 0
            }
        };

        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = job.Id, JobName = job.Name };
        await _runRepo.CreateAsync(run);

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new ServiceComparisonEngine(job, run, _runRepo, _diffRepo, streamA: streamA, streamB: streamB);
        var completedRun = await engine.RunAsync();

        Assert.AreEqual(3, completedRun.TotalDifferences);
        Assert.AreEqual(3, completedRun.InFlightDifferencesCount);
        Assert.AreEqual(0, completedRun.FilteredDifferencesCount);
        Assert.AreEqual(0, completedRun.MissingInBucketBCount, "Confirmed MissingInBucketBCount must remain 0 for in-flight items");
        Assert.AreEqual(0, completedRun.SizeMismatchCount, "Confirmed SizeMismatchCount must remain 0 for in-flight items");

        var alertService = new S3CompareService.Core.Notifications.AlertNotificationService();
        var (shouldAlert, _) = await alertService.EvaluateAlertConditionAsync(job, completedRun);
        Assert.IsFalse(shouldAlert, "In-flight differences must not trigger per-type disparity threshold alerts");
    }
}
