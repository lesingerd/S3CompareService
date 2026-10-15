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
}
