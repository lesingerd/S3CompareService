using Dapper;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Tests;

/// <summary>Schema version 3: composite run index, DifferencesTruncated column, incremental auto_vacuum on new files.</summary>
[TestClass]
public class RunIndexMigrationTests : TestDbBase
{
    private async Task<List<string>> IndexesOfScanRunsAsync() =>
        (await QueryAsync<string>("SELECT name FROM pragma_index_list('ScanRuns')")).ToList();

    private async Task<IEnumerable<T>> QueryAsync<T>(string sql)
    {
        await using var conn = await Factory.CreateOpenConnectionAsync();
        return await conn.QueryAsync<T>(sql);
    }

    [TestMethod]
    public async Task FreshDatabase_HasTheCompositeIndex_TheTruncatedColumn_AndIncrementalAutoVacuum()
    {
        var indexes = await IndexesOfScanRunsAsync();
        CollectionAssert.Contains(indexes, "IX_ScanRuns_JobId_StartTimeUtc");
        CollectionAssert.DoesNotContain(indexes, "IX_ScanRuns_JobId", "the single-column index is a prefix of the composite one");

        Assert.AreEqual(DatabaseMigrator.LatestVersion, await RawScalarAsync<int>("PRAGMA user_version"));
        var columns = (await QueryAsync<string>("SELECT name FROM pragma_table_info('ScanRuns')")).ToList();
        CollectionAssert.Contains(columns, "DifferencesTruncated");

        Assert.AreEqual(2, await RawScalarAsync<int>("PRAGMA auto_vacuum"), "2 = INCREMENTAL on a brand-new database");
    }

    [TestMethod]
    public async Task RecentRunsQuery_UsesTheCompositeIndex()
    {
        await using var conn = await Factory.CreateOpenConnectionAsync();
        var rows = await conn.QueryAsync("EXPLAIN QUERY PLAN SELECT Id FROM ScanRuns WHERE JobId = 'j' ORDER BY StartTimeUtc DESC LIMIT 1");
        var plan = string.Join(" | ", rows.Select(r => (string)((IDictionary<string, object>)r)["detail"]));
        StringAssert.Contains(plan, "IX_ScanRuns_JobId_StartTimeUtc");
        Assert.IsFalse(plan.Contains("TEMP B-TREE"), "no separate sort step: " + plan);
    }

    [TestMethod]
    public async Task ExistingVersion2Database_IsUpgraded_AndKeepsItsRows()
    {
        var job = new ComparisonJob { Name = "legacy" };
        await JobRepo.SaveAsync(job);
        var run = new ScanRun { JobId = job.Id, JobName = job.Name, Status = RunStatus.Completed, StartTimeUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc) };
        await RunRepo.CreateAsync(run);

        // Rewind the schema to what version 2 looked like.
        await using (var conn = await Factory.CreateOpenConnectionAsync())
        {
            await conn.ExecuteAsync("DROP INDEX IX_ScanRuns_JobId_StartTimeUtc; CREATE INDEX IX_ScanRuns_JobId ON ScanRuns (JobId); ALTER TABLE ScanRuns DROP COLUMN DifferencesTruncated; PRAGMA user_version = 2;");
        }

        await new DatabaseMigrator(Factory).MigrateAsync();

        var indexes = await IndexesOfScanRunsAsync();
        CollectionAssert.Contains(indexes, "IX_ScanRuns_JobId_StartTimeUtc");
        CollectionAssert.DoesNotContain(indexes, "IX_ScanRuns_JobId");
        Assert.AreEqual(3, await RawScalarAsync<int>("PRAGMA user_version"));
        var loaded = await RunRepo.GetByIdAsync(run.Id);
        Assert.IsNotNull(loaded);
        Assert.IsFalse(loaded.DifferencesTruncated);
        Assert.AreEqual(run.StartTimeUtc, await RunRepo.GetLatestStartTimeAsync(job.Id));
        Assert.IsNull(await RunRepo.GetLatestStartTimeAsync("no-such-job"));
    }

    [TestMethod]
    public async Task Summaries_AreTheSameRuns_WithoutTheWideTextColumns()
    {
        var job = new ComparisonJob { Name = "j" };
        await JobRepo.SaveAsync(job);
        var run = new ScanRun
        {
            JobId = job.Id, JobName = job.Name, Status = RunStatus.Completed, ObjectsCompared = 42, DifferencesTruncated = true,
            LastEvaluatedKey = "some/long/key", ScanWindowNotes = "notes", ErrorMessage = "kept"
        };
        await RunRepo.CreateAsync(run);

        var full = (await RunRepo.GetRecentRunsAsync(5, job.Id)).Single();
        var summary = (await RunRepo.GetRecentRunSummariesAsync(5, job.Id)).Single();
        Assert.AreEqual("some/long/key", full.LastEvaluatedKey);
        Assert.AreEqual("notes", full.ScanWindowNotes);
        Assert.AreEqual(string.Empty, summary.LastEvaluatedKey);
        Assert.AreEqual(string.Empty, summary.ScanWindowNotes);
        Assert.AreEqual(42, summary.ObjectsCompared);
        Assert.IsTrue(summary.DifferencesTruncated);
        Assert.AreEqual("kept", summary.ErrorMessage);
    }
}
