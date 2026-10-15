using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

[TestClass]
public class SqliteRepositoryTests
{
    private string _testDbPath = string.Empty;
    private SqliteConnectionFactory _factory = null!;
    private JobRepository _jobRepo = null!;
    private ScanRunRepository _runRepo = null!;
    private DifferenceRepository _diffRepo = null!;
    private AuditLogRepository _auditRepo = null!;

    [TestInitialize]
    public async Task Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"s3repo_{Guid.NewGuid():N}.db");
        _factory = new SqliteConnectionFactory($"Data Source={_testDbPath};Mode=ReadWriteCreate;");
        var migrator = new DatabaseMigrator(_factory);
        await migrator.MigrateAsync();

        _jobRepo = new JobRepository(_factory);
        _runRepo = new ScanRunRepository(_factory);
        _diffRepo = new DifferenceRepository(_factory);
        _auditRepo = new AuditLogRepository(_factory);
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
    public async Task JobRepository_Crud_WorksAccurately()
    {
        var job = new ComparisonJob
        {
            Name = "Production Mirror Job",
            Description = "Audit between US-East-1 and EU-Central-1",
            IntervalMinutes = 120,
            ReplicationGracePeriodMinutes = 30,
            IsEnabled = true
        };
        job.BucketA.BucketName = "prod-bucket-a";
        job.BucketB.BucketName = "prod-bucket-b";

        await _jobRepo.SaveAsync(job);

        var retrieved = await _jobRepo.GetByIdAsync(job.Id);
        Assert.IsNotNull(retrieved);
        Assert.AreEqual("Production Mirror Job", retrieved.Name);
        Assert.AreEqual("prod-bucket-a", retrieved.BucketA.BucketName);
        Assert.AreEqual(30, retrieved.ReplicationGracePeriodMinutes);

        // Update
        retrieved.Name = "Updated Production Mirror";
        await _jobRepo.SaveAsync(retrieved);

        var updated = await _jobRepo.GetByIdAsync(job.Id);
        Assert.IsNotNull(updated);
        Assert.AreEqual("Updated Production Mirror", updated.Name);

        // Delete
        await _jobRepo.DeleteAsync(job.Id);
        var deleted = await _jobRepo.GetByIdAsync(job.Id);
        Assert.IsNull(deleted);
    }

    [TestMethod]
    public async Task ScanRunRepository_CheckpointAndMetadataSnapshots_WorkAccurately()
    {
        var run = new ScanRun
        {
            Id = Guid.NewGuid().ToString("N"),
            JobId = "job-123",
            JobName = "Test Run",
            Status = RunStatus.Running,
            ObjectsCompared = 50000,
            LastEvaluatedKey = "folder/image_50000.jpg"
        };
        await _runRepo.CreateAsync(run);

        // Save Checkpoint
        var cp = new ScanCheckpoint
        {
            ScanRunId = run.Id,
            JobId = run.JobId,
            LastEvaluatedKey = "folder/image_50000.jpg",
            ObjectsCompared = 50000
        };
        await _runRepo.SaveCheckpointAsync(cp);

        var retrievedCp = await _runRepo.GetCheckpointAsync(run.Id);
        Assert.IsNotNull(retrievedCp);
        Assert.AreEqual("folder/image_50000.jpg", retrievedCp.LastEvaluatedKey);
        Assert.AreEqual(50000, retrievedCp.ObjectsCompared);

        // Save Snapshot
        var snapA = new BucketMetadataSnapshot
        {
            ScanRunId = run.Id,
            BucketSide = "A",
            BucketName = "bucket-a",
            TotalObjects = 50000,
            TotalBytes = 104857600
        };
        await _runRepo.SaveMetadataSnapshotAsync(snapA);

        var snapshots = await _runRepo.GetMetadataSnapshotsAsync(run.Id);
        Assert.AreEqual(1, snapshots.Count);
        Assert.AreEqual("bucket-a", snapshots[0].BucketName);

        // Delete checkpoint on completion
        await _runRepo.DeleteCheckpointAsync(run.Id);
        var deletedCp = await _runRepo.GetCheckpointAsync(run.Id);
        Assert.IsNull(deletedCp);
    }

    [TestMethod]
    public async Task DifferenceRepository_BatchInsertAndFiltering_WorksProperly()
    {
        string runId = Guid.NewGuid().ToString("N");
        var diffs = new List<RecordedDifference>();

        for (int i = 1; i <= 20; i++)
        {
            diffs.Add(new RecordedDifference
            {
                ScanRunId = runId,
                Key = $"file_{i:D3}.txt",
                Type = i % 2 == 0 ? DifferenceType.MissingInBucketB : DifferenceType.SizeMismatch,
                IsWithinGracePeriod = i <= 5, // first 5 are in-flight replication
                Details = $"Difference item {i}"
            });
        }

        await _diffRepo.InsertBatchAsync(diffs);

        // All items
        int total = await _diffRepo.GetDifferenceCountAsync(runId);
        Assert.AreEqual(20, total);

        // Filter: Confirmed disparities only (exclude grace period)
        int confirmedCount = await _diffRepo.GetDifferenceCountAsync(runId, withinGracePeriodFilter: false);
        Assert.AreEqual(15, confirmedCount);

        // Filter: In-flight replication only
        int inFlightCount = await _diffRepo.GetDifferenceCountAsync(runId, withinGracePeriodFilter: true);
        Assert.AreEqual(5, inFlightCount);

        // Paging
        var page = await _diffRepo.GetDifferencesAsync(runId, skip: 0, take: 10);
        Assert.AreEqual(10, page.Count);
        Assert.AreEqual("file_001.txt", page[0].Key);
    }
}
