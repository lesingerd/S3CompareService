using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

[TestClass]
public class StreamingComparisonTests
{
    private string _testDbPath = string.Empty;
    private SqliteConnectionFactory _factory = null!;
    private ScanRunRepository _runRepo = null!;
    private DifferenceRepository _diffRepo = null!;
    private JobRepository _jobRepo = null!;

    [TestInitialize]
    public async Task Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"s3test_{Guid.NewGuid():N}.db");
        _factory = new SqliteConnectionFactory($"Data Source={_testDbPath};Mode=ReadWriteCreate;");
        var migrator = new DatabaseMigrator(_factory);
        await migrator.MigrateAsync();

        _runRepo = new ScanRunRepository(_factory);
        _diffRepo = new DifferenceRepository(_factory);
        _jobRepo = new JobRepository(_factory);
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
    public async Task Compare_IdenticalBuckets_AllMatchedZeroDifferences()
    {
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "doc/a.txt", Size = 100, ETag = "etag1", LastModified = DateTime.UtcNow.AddHours(-5) },
            new() { Key = "doc/b.txt", Size = 200, ETag = "etag2", LastModified = DateTime.UtcNow.AddHours(-5) }
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "doc/a.txt", Size = 100, ETag = "etag1", LastModified = DateTime.UtcNow.AddHours(-5) },
            new() { Key = "doc/b.txt", Size = 200, ETag = "etag2", LastModified = DateTime.UtcNow.AddHours(-5) }
        };

        var job = new ComparisonJob { Name = "Test Match", ReplicationGracePeriodMinutes = 0 };
        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = job.Id, JobName = job.Name };
        await _runRepo.CreateAsync(run);

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new ServiceComparisonEngine(job, run, _runRepo, _diffRepo, streamA: streamA, streamB: streamB);
        var completedRun = await engine.RunAsync();

        Assert.AreEqual(RunStatus.Completed, completedRun.Status);
        Assert.AreEqual(2, completedRun.ObjectsCompared);
        Assert.AreEqual(2, completedRun.MatchedCount);
        Assert.AreEqual(0, completedRun.TotalDifferences);
    }

    [TestMethod]
    public async Task Compare_MissingInBucketB_And_MissingInBucketA_DetectedProperly()
    {
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "file1.dat", Size = 500, ETag = "tag1", LastModified = DateTime.UtcNow.AddHours(-5) },
            new() { Key = "file2.dat", Size = 600, ETag = "tag2", LastModified = DateTime.UtcNow.AddHours(-5) } // Missing in B
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "file1.dat", Size = 500, ETag = "tag1", LastModified = DateTime.UtcNow.AddHours(-5) },
            new() { Key = "file3.dat", Size = 700, ETag = "tag3", LastModified = DateTime.UtcNow.AddHours(-5) } // Missing in A
        };

        var job = new ComparisonJob { Name = "Test Asymmetric", ReplicationGracePeriodMinutes = 0 };
        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = job.Id, JobName = job.Name };
        await _runRepo.CreateAsync(run);

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new ServiceComparisonEngine(job, run, _runRepo, _diffRepo, streamA: streamA, streamB: streamB);
        var completedRun = await engine.RunAsync();

        Assert.AreEqual(3, completedRun.ObjectsCompared);
        Assert.AreEqual(1, completedRun.MatchedCount);
        Assert.AreEqual(2, completedRun.TotalDifferences);
        Assert.AreEqual(1, completedRun.MissingInBucketBCount);
        Assert.AreEqual(1, completedRun.MissingInBucketACount);
    }

    [TestMethod]
    public async Task Compare_SizeMismatch_DetectedProperly()
    {
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "data.bin", Size = 1024, ETag = "tagA", LastModified = DateTime.UtcNow.AddHours(-5) }
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "data.bin", Size = 2048, ETag = "tagB", LastModified = DateTime.UtcNow.AddHours(-5) }
        };

        var job = new ComparisonJob { Name = "Test Size", ReplicationGracePeriodMinutes = 0 };
        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = job.Id, JobName = job.Name };
        await _runRepo.CreateAsync(run);

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new ServiceComparisonEngine(job, run, _runRepo, _diffRepo, streamA: streamA, streamB: streamB);
        var completedRun = await engine.RunAsync();

        Assert.AreEqual(1, completedRun.ObjectsCompared);
        Assert.AreEqual(0, completedRun.MatchedCount);
        Assert.AreEqual(1, completedRun.SizeMismatchCount);
        Assert.AreEqual(1, completedRun.TotalDifferences);
    }

    [TestMethod]
    public async Task Compare_SequenceGapDetection_FindsMissingNumericItems()
    {
        // Series has 001 and 003, skipping 002
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "logs/chunk_001.log", Size = 100, ETag = "tag1", LastModified = DateTime.UtcNow.AddHours(-5) },
            new() { Key = "logs/chunk_003.log", Size = 100, ETag = "tag3", LastModified = DateTime.UtcNow.AddHours(-5) }
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "logs/chunk_001.log", Size = 100, ETag = "tag1", LastModified = DateTime.UtcNow.AddHours(-5) },
            new() { Key = "logs/chunk_003.log", Size = 100, ETag = "tag3", LastModified = DateTime.UtcNow.AddHours(-5) }
        };

        var job = new ComparisonJob { Name = "Test Gap", ReplicationGracePeriodMinutes = 0, DetectSequenceGaps = true, MaxSequenceGapThreshold = 10 };
        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = job.Id, JobName = job.Name };
        await _runRepo.CreateAsync(run);

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new ServiceComparisonEngine(job, run, _runRepo, _diffRepo, streamA: streamA, streamB: streamB);
        var completedRun = await engine.RunAsync();

        Assert.AreEqual(1, completedRun.SequenceGapCount);
        var diffs = await _diffRepo.GetAllForRunAsync(completedRun.Id);
        var gapDiff = diffs.FirstOrDefault(d => d.Type == DifferenceType.PossibleMissingOnBothSides);
        Assert.IsNotNull(gapDiff);
        Assert.AreEqual("logs/chunk_002.log", gapDiff.Key);
    }

    [TestMethod]
    public async Task Compare_AsymmetricBucketPrefixes_MatchesRelativeKeysAndRecordsRelativeDifferenceKey()
    {
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "prod/tenant1/doc/a.txt", Size = 100, ETag = "etag1", LastModified = DateTime.UtcNow.AddHours(-5), StorageClass = "STANDARD" },
            new() { Key = "prod/tenant1/doc/only_in_a.txt", Size = 250, ETag = "etag2", LastModified = DateTime.UtcNow.AddHours(-5), StorageClass = "GLACIER" }
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "backup/tenant1/doc/a.txt", Size = 100, ETag = "etag1", LastModified = DateTime.UtcNow.AddHours(-5), StorageClass = "STANDARD_IA" }
        };

        var job = new ComparisonJob
        {
            Name = "Asymmetric Prefix Job",
            ReplicationGracePeriodMinutes = 0,
            BucketA = new S3BucketConfig { BucketName = "bucket-a", Prefix = "prod/tenant1/" },
            BucketB = new S3BucketConfig { BucketName = "bucket-b", Prefix = "backup/tenant1/" }
        };
        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = job.Id, JobName = job.Name };
        await _runRepo.CreateAsync(run);

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new ServiceComparisonEngine(job, run, _runRepo, _diffRepo, streamA: streamA, streamB: streamB);
        var completedRun = await engine.RunAsync();

        Assert.AreEqual(RunStatus.Completed, completedRun.Status);
        Assert.AreEqual(2, completedRun.ObjectsCompared);
        Assert.AreEqual(1, completedRun.MatchedCount);
        Assert.AreEqual(1, completedRun.MissingInBucketBCount);

        var diffs = await _diffRepo.GetAllForRunAsync(completedRun.Id);
        Assert.AreEqual(1, diffs.Count);
        Assert.AreEqual("doc/only_in_a.txt", diffs[0].Key);

        // Verify metadata snapshots captured date ranges and storage classes
        var snapshots = await _runRepo.GetMetadataSnapshotsAsync(completedRun.Id);
        Assert.AreEqual(2, snapshots.Count);
        var snapA = snapshots.First(s => s.BucketSide == "A");
        Assert.IsNotNull(snapA.OldestLastModifiedUtc);
        Assert.IsNotNull(snapA.NewestLastModifiedUtc);
        Assert.IsTrue(snapA.StorageClassesJson.Contains("STANDARD"));
        Assert.IsTrue(snapA.StorageClassesJson.Contains("GLACIER"));
    }

    [TestMethod]
    public async Task Compare_GracefulServiceShutdown_MarksRunAsInterrupted()
    {
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "doc/a.txt", Size = 100, ETag = "etag1", LastModified = DateTime.UtcNow.AddHours(-5) }
        };
        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "doc/a.txt", Size = 100, ETag = "etag1", LastModified = DateTime.UtcNow.AddHours(-5) }
        };

        var job = new ComparisonJob { Name = "Shutdown Test Job", ReplicationGracePeriodMinutes = 0 };
        var run = new ScanRun { Id = Guid.NewGuid().ToString("N"), JobId = job.Id, JobName = job.Name };
        await _runRepo.CreateAsync(run);

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new ServiceComparisonEngine(job, run, _runRepo, _diffRepo, streamA: streamA, streamB: streamB);
        using var cts = new CancellationTokenSource();
        engine.MarkServiceShutdown();
        cts.Cancel();

        try
        {
            await engine.RunAsync(cts.Token);
            Assert.Fail("Expected OperationCanceledException");
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown cancellation
        }

        var savedRun = await _runRepo.GetByIdAsync(run.Id);
        Assert.IsNotNull(savedRun);
        Assert.AreEqual(RunStatus.Interrupted, savedRun.Status);
        Assert.IsTrue(savedRun.ErrorMessage!.Contains("service shutdown", StringComparison.OrdinalIgnoreCase));
    }
}
