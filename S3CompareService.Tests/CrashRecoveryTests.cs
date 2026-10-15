using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.Services;

namespace S3CompareService.Tests;

[TestClass]
public class CrashRecoveryTests
{
    private string _testDbPath = string.Empty;
    private SqliteConnectionFactory _factory = null!;
    private ScanRunRepository _runRepo = null!;
    private JobRepository _jobRepo = null!;
    private DifferenceRepository _diffRepo = null!;
    private AuditLogRepository _auditRepo = null!;

    [TestInitialize]
    public async Task Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"s3crash_{Guid.NewGuid():N}.db");
        _factory = new SqliteConnectionFactory($"Data Source={_testDbPath};Mode=ReadWriteCreate;");
        var migrator = new DatabaseMigrator(_factory);
        await migrator.MigrateAsync();

        _runRepo = new ScanRunRepository(_factory);
        _jobRepo = new JobRepository(_factory);
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
    public async Task CrashRecovery_DetectsUnfinishedRuns_MarksAsInterrupted_AndLogsRecoveryEvent()
    {
        var job = new ComparisonJob { Name = "Crash Recovery Test Job", IsEnabled = false };
        await _jobRepo.SaveAsync(job);

        // Simulate a crashed run that was in status 'Running'
        var crashedRun = new ScanRun
        {
            Id = Guid.NewGuid().ToString("N"),
            JobId = job.Id,
            JobName = job.Name,
            Status = RunStatus.Running,
            ObjectsCompared = 12500,
            LastEvaluatedKey = "folder/part_012500.csv"
        };
        await _runRepo.CreateAsync(crashedRun);

        // Simulate a checkpoint saved before crash
        var checkpoint = new ScanCheckpoint
        {
            ScanRunId = crashedRun.Id,
            JobId = job.Id,
            LastEvaluatedKey = "folder/part_012500.csv",
            ObjectsCompared = 12500
        };
        await _runRepo.SaveCheckpointAsync(checkpoint);

        // Verify it shows up in unfinished runs
        var unfinished = await _runRepo.GetUnfinishedRunsAsync();
        Assert.AreEqual(1, unfinished.Count);

        // Run recovery manager
        var alertService = new S3CompareService.Core.Notifications.AlertNotificationService(_auditRepo);
        var coordinator = new ScanExecutionCoordinator(_jobRepo, _runRepo, _diffRepo, _auditRepo, alertService, NullLogger<ScanExecutionCoordinator>.Instance);
        var recovery = new CrashRecoveryManager(_runRepo, _jobRepo, _auditRepo, coordinator, NullLogger<CrashRecoveryManager>.Instance);

        await recovery.RunStartupRecoveryAsync();

        // Verify status was changed from Running to Interrupted
        var recoveredRun = await _runRepo.GetByIdAsync(crashedRun.Id);
        Assert.IsNotNull(recoveredRun);
        Assert.AreEqual(RunStatus.Interrupted, recoveredRun.Status);
        Assert.IsTrue(recoveredRun.ErrorMessage!.Contains("interrupted by service restart"));

        // Verify audit log recorded recovery event
        var logs = await _auditRepo.GetRecentLogsAsync(10);
        var recoveryLog = logs.FirstOrDefault(l => l.Level == "RECOVERY");
        Assert.IsNotNull(recoveryLog);
        Assert.IsTrue(recoveryLog.Message.Contains(crashedRun.Id));
    }
}
