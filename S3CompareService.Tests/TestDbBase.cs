using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

/// <summary>Shared fixture: a fresh migrated SQLite database per test.</summary>
public abstract class TestDbBase
{
    protected string DbPath = string.Empty;
    protected SqliteConnectionFactory Factory = null!;
    protected JobRepository JobRepo = null!;
    protected ScanRunRepository RunRepo = null!;
    protected DifferenceRepository DiffRepo = null!;
    protected AuditLogRepository AuditRepo = null!;

    [TestInitialize]
    public async Task BaseSetup()
    {
        DbPath = Path.Combine(Path.GetTempPath(), $"s3hard_{Guid.NewGuid():N}.db");
        Factory = new SqliteConnectionFactory($"Data Source={DbPath};Mode=ReadWriteCreate;");
        await new DatabaseMigrator(Factory).MigrateAsync();

        JobRepo = new JobRepository(Factory);
        RunRepo = new ScanRunRepository(Factory);
        DiffRepo = new DifferenceRepository(Factory);
        AuditRepo = new AuditLogRepository(Factory);
    }

    [TestCleanup]
    public void BaseCleanup()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-shm", "-wal" })
        {
            try { File.Delete(DbPath + suffix); } catch { }
        }
    }

    /// <summary>Reads a raw column value bypassing the repositories (to prove what is really stored).</summary>
    protected async Task<T?> RawScalarAsync<T>(string sql, object? param = null)
    {
        await using var conn = await Factory.CreateOpenConnectionAsync();
        return await Dapper.SqlMapper.ExecuteScalarAsync<T>(conn, sql, param);
    }
}
