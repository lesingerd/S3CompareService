using System.Data;
using System.Text.Json;
using Dapper;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Storage;

public interface IScanRunRepository
{
    Task CreateAsync(ScanRun run, CancellationToken cancellationToken = default);
    Task UpdateProgressAsync(ScanRun run, CancellationToken cancellationToken = default);
    Task CompleteAsync(ScanRun run, CancellationToken cancellationToken = default);
    Task<ScanRun?> GetByIdAsync(string id, CancellationToken cancellationToken = default);
    Task<List<ScanRun>> GetRecentRunsAsync(int count = 50, string? jobId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs that did not finish: Status Running, Paused or Interrupted, excluding runs already superseded by a
    /// newer run (<see cref="ScanRun.SupersededByRunId"/>).
    /// </summary>
    Task<List<ScanRun>> GetUnfinishedRunsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Resume-into-same-run support. Atomically flips an unfinished run (Running/Paused/Interrupted/Failed, not
    /// superseded) back to Running, clears EndTimeUtc/ErrorMessage, sets IsResumedFromCheckpoint, ResumedFromKey
    /// (= the run's LastEvaluatedKey) and increments ResumeCount. Returns the re-read run, or null if the run
    /// does not exist or is not reopenable. Pass the returned run, together with GetCheckpointAsync(run.Id), to
    /// <c>ServiceComparisonEngine</c> so differences stay under the original run id.
    /// </summary>
    Task<ScanRun?> ReopenForResumeAsync(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Prepares the database for an engine resuming at <paramref name="lastEvaluatedKey"/>: deletes differences of
    /// <paramref name="runId"/> whose (relative) Key sorts after it (they may have been flushed after the last
    /// checkpoint and would otherwise be duplicated). If <paramref name="fromRunId"/> differs from
    /// <paramref name="runId"/> (legacy "new run id" resume) the old run's differences are first re-assigned to
    /// <paramref name="runId"/>, the old run is marked superseded and its checkpoint deleted. Returns the number
    /// of differences deleted.
    /// </summary>
    Task<int> PrepareResumeAsync(string runId, string? fromRunId, string lastEvaluatedKey, CancellationToken cancellationToken = default);

    /// <summary>Deletes all differences, snapshots and the checkpoint of a run and zeroes its counters (fresh start).</summary>
    Task ResetRunDataAsync(string runId, CancellationToken cancellationToken = default);

    Task SaveMetadataSnapshotAsync(BucketMetadataSnapshot snapshot, CancellationToken cancellationToken = default);
    Task<List<BucketMetadataSnapshot>> GetMetadataSnapshotsAsync(string scanRunId, CancellationToken cancellationToken = default);

    Task SaveCheckpointAsync(ScanCheckpoint checkpoint, CancellationToken cancellationToken = default);
    Task<ScanCheckpoint?> GetCheckpointAsync(string scanRunId, CancellationToken cancellationToken = default);
    Task DeleteCheckpointAsync(string scanRunId, CancellationToken cancellationToken = default);
}

public class ScanRunRepository : IScanRunRepository
{
    private readonly SqliteConnectionFactory _factory;

    public ScanRunRepository(SqliteConnectionFactory factory)
    {
        _factory = factory;
    }

    private static object BuildParams(ScanRun run) => new
    {
        run.Id,
        run.JobId,
        run.JobName,
        StartTimeUtc = DbDateTime.ToDb(run.StartTimeUtc),
        EndTimeUtc = DbDateTime.ToDb(run.EndTimeUtc),
        Status = (int)run.Status,
        Mode = (int)run.Mode,
        run.ReplicationGracePeriodMinutes,
        GracePeriodCutoffUtc = DbDateTime.ToDb(run.GracePeriodCutoffUtc),
        IsScanWindowClipped = run.IsScanWindowClipped ? 1 : 0,
        run.ScanWindowNotes,
        run.LastEvaluatedKey,
        IsResumedFromCheckpoint = run.IsResumedFromCheckpoint ? 1 : 0,
        run.ResumedFromKey,
        run.ResumeCount,
        run.SupersededByRunId,
        run.ObjectsCompared,
        run.MatchedCount,
        run.TotalDifferences,
        run.FilteredDifferencesCount,
        run.InFlightDifferencesCount,
        run.MissingInBucketBCount,
        run.MissingInBucketACount,
        run.SizeMismatchCount,
        run.ETagMismatchCount,
        run.ContentMismatchCount,
        run.SequenceGapCount,
        run.BucketAObjectsRead,
        run.BucketBObjectsRead,
        run.BucketABytesRead,
        run.BucketBBytesRead,
        run.DurationSeconds,
        run.ErrorMessage
    };

    public async Task CreateAsync(ScanRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = @"
            INSERT INTO ScanRuns (
                Id, JobId, JobName, StartTimeUtc, EndTimeUtc, Status, Mode,
                ReplicationGracePeriodMinutes, GracePeriodCutoffUtc, IsScanWindowClipped,
                ScanWindowNotes, LastEvaluatedKey, IsResumedFromCheckpoint, ResumedFromKey,
                ResumeCount, SupersededByRunId,
                ObjectsCompared, MatchedCount, TotalDifferences, FilteredDifferencesCount,
                InFlightDifferencesCount, MissingInBucketBCount, MissingInBucketACount,
                SizeMismatchCount, ETagMismatchCount, ContentMismatchCount, SequenceGapCount,
                BucketAObjectsRead, BucketBObjectsRead, BucketABytesRead, BucketBBytesRead,
                DurationSeconds, ErrorMessage
            ) VALUES (
                @Id, @JobId, @JobName, @StartTimeUtc, @EndTimeUtc, @Status, @Mode,
                @ReplicationGracePeriodMinutes, @GracePeriodCutoffUtc, @IsScanWindowClipped,
                @ScanWindowNotes, @LastEvaluatedKey, @IsResumedFromCheckpoint, @ResumedFromKey,
                @ResumeCount, @SupersededByRunId,
                @ObjectsCompared, @MatchedCount, @TotalDifferences, @FilteredDifferencesCount,
                @InFlightDifferencesCount, @MissingInBucketBCount, @MissingInBucketACount,
                @SizeMismatchCount, @ETagMismatchCount, @ContentMismatchCount, @SequenceGapCount,
                @BucketAObjectsRead, @BucketBObjectsRead, @BucketABytesRead, @BucketBBytesRead,
                @DurationSeconds, @ErrorMessage
            );
        ";

        await conn.ExecuteAsync(new CommandDefinition(sql, BuildParams(run), cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    // Shared SET list: counters plus the run metadata the engine sets at start (start time, grace window, resume info).
    private const string ProgressSetClause = @"
                Status = @Status,
                StartTimeUtc = @StartTimeUtc,
                ReplicationGracePeriodMinutes = @ReplicationGracePeriodMinutes,
                GracePeriodCutoffUtc = @GracePeriodCutoffUtc,
                IsScanWindowClipped = @IsScanWindowClipped,
                ScanWindowNotes = @ScanWindowNotes,
                IsResumedFromCheckpoint = @IsResumedFromCheckpoint,
                ResumedFromKey = @ResumedFromKey,
                ResumeCount = @ResumeCount,
                LastEvaluatedKey = @LastEvaluatedKey,
                ObjectsCompared = @ObjectsCompared,
                MatchedCount = @MatchedCount,
                TotalDifferences = @TotalDifferences,
                FilteredDifferencesCount = @FilteredDifferencesCount,
                InFlightDifferencesCount = @InFlightDifferencesCount,
                MissingInBucketBCount = @MissingInBucketBCount,
                MissingInBucketACount = @MissingInBucketACount,
                SizeMismatchCount = @SizeMismatchCount,
                ETagMismatchCount = @ETagMismatchCount,
                ContentMismatchCount = @ContentMismatchCount,
                SequenceGapCount = @SequenceGapCount,
                BucketAObjectsRead = @BucketAObjectsRead,
                BucketBObjectsRead = @BucketBObjectsRead,
                BucketABytesRead = @BucketABytesRead,
                BucketBBytesRead = @BucketBBytesRead,
                DurationSeconds = @DurationSeconds,
                ErrorMessage = @ErrorMessage";

    public async Task UpdateProgressAsync(ScanRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = $"UPDATE ScanRuns SET {ProgressSetClause} WHERE Id = @Id;";
        await conn.ExecuteAsync(new CommandDefinition(sql, BuildParams(run), cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task CompleteAsync(ScanRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = $"UPDATE ScanRuns SET EndTimeUtc = @EndTimeUtc, {ProgressSetClause} WHERE Id = @Id;";
        await conn.ExecuteAsync(new CommandDefinition(sql, BuildParams(run), cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task<ScanRun?> GetByIdAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = "SELECT * FROM ScanRuns WHERE Id = @Id";
        var row = await conn.QuerySingleOrDefaultAsync(new CommandDefinition(sql, new { Id = id }, cancellationToken: cancellationToken)).ConfigureAwait(false);
        return row != null ? MapRowToScanRun(row) : null;
    }

    public async Task<List<ScanRun>> GetRecentRunsAsync(int count = 50, string? jobId = null, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = string.IsNullOrWhiteSpace(jobId)
            ? "SELECT * FROM ScanRuns ORDER BY StartTimeUtc DESC LIMIT @Count"
            : "SELECT * FROM ScanRuns WHERE JobId = @JobId ORDER BY StartTimeUtc DESC LIMIT @Count";

        var rows = await conn.QueryAsync(new CommandDefinition(sql, new { Count = count, JobId = jobId }, cancellationToken: cancellationToken)).ConfigureAwait(false);
        var list = new List<ScanRun>();
        foreach (var r in rows)
        {
            list.Add(MapRowToScanRun(r));
        }
        return list;
    }

    public async Task<List<ScanRun>> GetUnfinishedRunsAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = @"SELECT * FROM ScanRuns
                       WHERE Status IN (@Running, @Paused, @Interrupted) AND SupersededByRunId IS NULL
                       ORDER BY StartTimeUtc ASC";
        var p = new { Running = (int)RunStatus.Running, Paused = (int)RunStatus.Paused, Interrupted = (int)RunStatus.Interrupted };
        var rows = await conn.QueryAsync(new CommandDefinition(sql, p, cancellationToken: cancellationToken)).ConfigureAwait(false);
        var list = new List<ScanRun>();
        foreach (var r in rows)
        {
            list.Add(MapRowToScanRun(r));
        }
        return list;
    }

    public async Task<ScanRun?> ReopenForResumeAsync(string runId, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = @"
            UPDATE ScanRuns SET
                Status = @Running,
                EndTimeUtc = NULL,
                ErrorMessage = NULL,
                IsResumedFromCheckpoint = 1,
                ResumedFromKey = CASE WHEN LastEvaluatedKey IS NOT NULL AND LastEvaluatedKey <> '' THEN LastEvaluatedKey ELSE ResumedFromKey END,
                ResumeCount = ResumeCount + 1
            WHERE Id = @Id
              AND SupersededByRunId IS NULL
              AND Status IN (@Running, @Paused, @Interrupted, @Failed);";
        var p = new
        {
            Id = runId,
            Running = (int)RunStatus.Running,
            Paused = (int)RunStatus.Paused,
            Interrupted = (int)RunStatus.Interrupted,
            Failed = (int)RunStatus.Failed
        };
        int rows = await conn.ExecuteAsync(new CommandDefinition(sql, p, cancellationToken: cancellationToken)).ConfigureAwait(false);
        if (rows == 0) return null;
        return await GetByIdAsync(runId, cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> PrepareResumeAsync(string runId, string? fromRunId, string lastEvaluatedKey, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var trans = await conn.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);

        if (!string.IsNullOrEmpty(fromRunId) && fromRunId != runId)
        {
            await conn.ExecuteAsync(new CommandDefinition(
                "UPDATE RecordedDifferences SET ScanRunId = @New WHERE ScanRunId = @Old",
                new { New = runId, Old = fromRunId }, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);
            await conn.ExecuteAsync(new CommandDefinition(
                "UPDATE ScanRuns SET SupersededByRunId = @New WHERE Id = @Old",
                new { New = runId, Old = fromRunId }, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);
            await conn.ExecuteAsync(new CommandDefinition(
                "DELETE FROM Checkpoints WHERE ScanRunId = @Old",
                new { Old = fromRunId }, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);
        }

        // SQLite's default BINARY collation compares UTF-8 bytes, i.e. the same order S3 lists keys in.
        int deleted = await conn.ExecuteAsync(new CommandDefinition(
            "DELETE FROM RecordedDifferences WHERE ScanRunId = @RunId AND Key > @Key",
            new { RunId = runId, Key = lastEvaluatedKey }, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);

        await trans.CommitAsync(cancellationToken).ConfigureAwait(false);
        return deleted;
    }

    public async Task ResetRunDataAsync(string runId, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var trans = await conn.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);
        var p = new { Id = runId };

        await conn.ExecuteAsync(new CommandDefinition("DELETE FROM RecordedDifferences WHERE ScanRunId = @Id", p, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);
        await conn.ExecuteAsync(new CommandDefinition("DELETE FROM BucketMetadataSnapshots WHERE ScanRunId = @Id", p, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);
        await conn.ExecuteAsync(new CommandDefinition("DELETE FROM Checkpoints WHERE ScanRunId = @Id", p, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);
        await conn.ExecuteAsync(new CommandDefinition(@"
            UPDATE ScanRuns SET
                LastEvaluatedKey = '', IsResumedFromCheckpoint = 0, ResumedFromKey = NULL,
                ObjectsCompared = 0, MatchedCount = 0, TotalDifferences = 0, FilteredDifferencesCount = 0,
                InFlightDifferencesCount = 0, MissingInBucketBCount = 0, MissingInBucketACount = 0,
                SizeMismatchCount = 0, ETagMismatchCount = 0, ContentMismatchCount = 0, SequenceGapCount = 0,
                BucketAObjectsRead = 0, BucketBObjectsRead = 0, BucketABytesRead = 0, BucketBBytesRead = 0,
                DurationSeconds = 0
            WHERE Id = @Id", p, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);

        await trans.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task SaveMetadataSnapshotAsync(BucketMetadataSnapshot snapshot, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = @"
            INSERT INTO BucketMetadataSnapshots (
                ScanRunId, BucketSide, BucketName, EndpointUrl, Region, Prefix,
                SnapshotTimeUtc, TotalObjects, TotalBytes, VersioningStatus,
                StorageClassesJson, OldestLastModifiedUtc, NewestLastModifiedUtc
            ) VALUES (
                @ScanRunId, @BucketSide, @BucketName, @EndpointUrl, @Region, @Prefix,
                @SnapshotTimeUtc, @TotalObjects, @TotalBytes, @VersioningStatus,
                @StorageClassesJson, @OldestLastModifiedUtc, @NewestLastModifiedUtc
            );
        ";

        var p = new
        {
            snapshot.ScanRunId,
            snapshot.BucketSide,
            snapshot.BucketName,
            snapshot.EndpointUrl,
            snapshot.Region,
            snapshot.Prefix,
            SnapshotTimeUtc = DbDateTime.ToDb(snapshot.SnapshotTimeUtc),
            snapshot.TotalObjects,
            snapshot.TotalBytes,
            snapshot.VersioningStatus,
            snapshot.StorageClassesJson,
            OldestLastModifiedUtc = DbDateTime.ToDb(snapshot.OldestLastModifiedUtc),
            NewestLastModifiedUtc = DbDateTime.ToDb(snapshot.NewestLastModifiedUtc)
        };

        await conn.ExecuteAsync(new CommandDefinition(sql, p, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task<List<BucketMetadataSnapshot>> GetMetadataSnapshotsAsync(string scanRunId, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = "SELECT * FROM BucketMetadataSnapshots WHERE ScanRunId = @ScanRunId ORDER BY BucketSide ASC";
        var rows = await conn.QueryAsync(new CommandDefinition(sql, new { ScanRunId = scanRunId }, cancellationToken: cancellationToken)).ConfigureAwait(false);
        var list = new List<BucketMetadataSnapshot>();
        foreach (var r in rows)
        {
            list.Add(new BucketMetadataSnapshot
            {
                Id = (long)r.Id,
                ScanRunId = (string)r.ScanRunId,
                BucketSide = (string)r.BucketSide,
                BucketName = (string)r.BucketName,
                EndpointUrl = (string)r.EndpointUrl,
                Region = (string)(r.Region ?? string.Empty),
                Prefix = (string)(r.Prefix ?? string.Empty),
                SnapshotTimeUtc = DbDateTime.Parse((object?)r.SnapshotTimeUtc),
                TotalObjects = (long)r.TotalObjects,
                TotalBytes = (long)r.TotalBytes,
                VersioningStatus = (string?)r.VersioningStatus,
                StorageClassesJson = (string)(r.StorageClassesJson ?? "{}"),
                OldestLastModifiedUtc = DbDateTime.ParseNullable((object?)r.OldestLastModifiedUtc),
                NewestLastModifiedUtc = DbDateTime.ParseNullable((object?)r.NewestLastModifiedUtc)
            });
        }
        return list;
    }

    public async Task SaveCheckpointAsync(ScanCheckpoint checkpoint, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        checkpoint.LastUpdatedAtUtc = DateTime.UtcNow;
        string json = JsonSerializer.Serialize(checkpoint);

        string sql = @"
            INSERT INTO Checkpoints (ScanRunId, JobId, CheckpointJson, LastUpdatedAtUtc)
            VALUES (@ScanRunId, @JobId, @CheckpointJson, @LastUpdatedAtUtc)
            ON CONFLICT(ScanRunId) DO UPDATE SET
                CheckpointJson = excluded.CheckpointJson,
                LastUpdatedAtUtc = excluded.LastUpdatedAtUtc;
        ";

        var p = new
        {
            checkpoint.ScanRunId,
            checkpoint.JobId,
            CheckpointJson = json,
            LastUpdatedAtUtc = DbDateTime.ToDb(checkpoint.LastUpdatedAtUtc)
        };

        await conn.ExecuteAsync(new CommandDefinition(sql, p, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task<ScanCheckpoint?> GetCheckpointAsync(string scanRunId, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = "SELECT CheckpointJson FROM Checkpoints WHERE ScanRunId = @ScanRunId";
        var json = await conn.QuerySingleOrDefaultAsync<string>(new CommandDefinition(sql, new { ScanRunId = scanRunId }, cancellationToken: cancellationToken)).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(json)) return null;

        var cp = JsonSerializer.Deserialize<ScanCheckpoint>(json);
        if (cp != null)
        {
            cp.CreatedAtUtc = DbDateTime.ToUtc(cp.CreatedAtUtc);
            cp.LastUpdatedAtUtc = DbDateTime.ToUtc(cp.LastUpdatedAtUtc);
        }
        return cp;
    }

    public async Task DeleteCheckpointAsync(string scanRunId, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = "DELETE FROM Checkpoints WHERE ScanRunId = @ScanRunId";
        await conn.ExecuteAsync(new CommandDefinition(sql, new { ScanRunId = scanRunId }, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    private static ScanRun MapRowToScanRun(dynamic r)
    {
        return new ScanRun
        {
            Id = (string)r.Id,
            JobId = (string)r.JobId,
            JobName = (string)r.JobName,
            StartTimeUtc = DbDateTime.Parse((object?)r.StartTimeUtc),
            EndTimeUtc = DbDateTime.ParseNullable((object?)r.EndTimeUtc),
            Status = (RunStatus)(int)r.Status,
            Mode = (ComparisonMode)(int)r.Mode,
            ReplicationGracePeriodMinutes = (int)r.ReplicationGracePeriodMinutes,
            GracePeriodCutoffUtc = DbDateTime.ParseNullable((object?)r.GracePeriodCutoffUtc),
            IsScanWindowClipped = ((long)r.IsScanWindowClipped) == 1,
            ScanWindowNotes = (string)(r.ScanWindowNotes ?? string.Empty),
            LastEvaluatedKey = (string)(r.LastEvaluatedKey ?? string.Empty),
            IsResumedFromCheckpoint = ((long)r.IsResumedFromCheckpoint) == 1,
            ResumedFromKey = (string?)r.ResumedFromKey,
            ResumeCount = (int)(long)r.ResumeCount,
            SupersededByRunId = (string?)r.SupersededByRunId,
            ObjectsCompared = (long)r.ObjectsCompared,
            MatchedCount = (long)r.MatchedCount,
            TotalDifferences = (long)r.TotalDifferences,
            FilteredDifferencesCount = (long)r.FilteredDifferencesCount,
            InFlightDifferencesCount = (long)r.InFlightDifferencesCount,
            MissingInBucketBCount = (long)r.MissingInBucketBCount,
            MissingInBucketACount = (long)r.MissingInBucketACount,
            SizeMismatchCount = (long)r.SizeMismatchCount,
            ETagMismatchCount = (long)r.ETagMismatchCount,
            ContentMismatchCount = (long)r.ContentMismatchCount,
            SequenceGapCount = (long)r.SequenceGapCount,
            BucketAObjectsRead = (long)r.BucketAObjectsRead,
            BucketBObjectsRead = (long)r.BucketBObjectsRead,
            BucketABytesRead = (long)r.BucketABytesRead,
            BucketBBytesRead = (long)r.BucketBBytesRead,
            DurationSeconds = (double)r.DurationSeconds,
            ErrorMessage = (string?)r.ErrorMessage
        };
    }
}
