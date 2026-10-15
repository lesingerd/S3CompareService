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
    Task<List<ScanRun>> GetUnfinishedRunsAsync(CancellationToken cancellationToken = default);

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

    public async Task CreateAsync(ScanRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = @"
            INSERT INTO ScanRuns (
                Id, JobId, JobName, StartTimeUtc, EndTimeUtc, Status, Mode,
                ReplicationGracePeriodMinutes, GracePeriodCutoffUtc, IsScanWindowClipped,
                ScanWindowNotes, LastEvaluatedKey, IsResumedFromCheckpoint, ResumedFromKey,
                ObjectsCompared, MatchedCount, TotalDifferences, FilteredDifferencesCount,
                InFlightDifferencesCount, MissingInBucketBCount, MissingInBucketACount,
                SizeMismatchCount, ETagMismatchCount, ContentMismatchCount, SequenceGapCount,
                BucketAObjectsRead, BucketBObjectsRead, BucketABytesRead, BucketBBytesRead,
                DurationSeconds, ErrorMessage
            ) VALUES (
                @Id, @JobId, @JobName, @StartTimeUtc, @EndTimeUtc, @Status, @Mode,
                @ReplicationGracePeriodMinutes, @GracePeriodCutoffUtc, @IsScanWindowClipped,
                @ScanWindowNotes, @LastEvaluatedKey, @IsResumedFromCheckpoint, @ResumedFromKey,
                @ObjectsCompared, @MatchedCount, @TotalDifferences, @FilteredDifferencesCount,
                @InFlightDifferencesCount, @MissingInBucketBCount, @MissingInBucketACount,
                @SizeMismatchCount, @ETagMismatchCount, @ContentMismatchCount, @SequenceGapCount,
                @BucketAObjectsRead, @BucketBObjectsRead, @BucketABytesRead, @BucketBBytesRead,
                @DurationSeconds, @ErrorMessage
            );
        ";

        var p = new
        {
            run.Id,
            run.JobId,
            run.JobName,
            StartTimeUtc = run.StartTimeUtc.ToString("O"),
            EndTimeUtc = run.EndTimeUtc?.ToString("O"),
            Status = (int)run.Status,
            Mode = (int)run.Mode,
            run.ReplicationGracePeriodMinutes,
            GracePeriodCutoffUtc = run.GracePeriodCutoffUtc?.ToString("O"),
            IsScanWindowClipped = run.IsScanWindowClipped ? 1 : 0,
            run.ScanWindowNotes,
            run.LastEvaluatedKey,
            IsResumedFromCheckpoint = run.IsResumedFromCheckpoint ? 1 : 0,
            run.ResumedFromKey,
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

        await conn.ExecuteAsync(new CommandDefinition(sql, p, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task UpdateProgressAsync(ScanRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = @"
            UPDATE ScanRuns SET
                Status = @Status,
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
                ErrorMessage = @ErrorMessage
            WHERE Id = @Id;
        ";

        var p = new
        {
            run.Id,
            Status = (int)run.Status,
            run.LastEvaluatedKey,
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

        await conn.ExecuteAsync(new CommandDefinition(sql, p, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task CompleteAsync(ScanRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = @"
            UPDATE ScanRuns SET
                EndTimeUtc = @EndTimeUtc,
                Status = @Status,
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
                ErrorMessage = @ErrorMessage
            WHERE Id = @Id;
        ";

        var p = new
        {
            run.Id,
            EndTimeUtc = run.EndTimeUtc?.ToString("O"),
            Status = (int)run.Status,
            run.LastEvaluatedKey,
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

        await conn.ExecuteAsync(new CommandDefinition(sql, p, cancellationToken: cancellationToken)).ConfigureAwait(false);
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
        // Status 1 = Running, Status 2 = Paused
        string sql = "SELECT * FROM ScanRuns WHERE Status IN (1, 2) ORDER BY StartTimeUtc ASC";
        var rows = await conn.QueryAsync(new CommandDefinition(sql, cancellationToken: cancellationToken)).ConfigureAwait(false);
        var list = new List<ScanRun>();
        foreach (var r in rows)
        {
            list.Add(MapRowToScanRun(r));
        }
        return list;
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
            SnapshotTimeUtc = snapshot.SnapshotTimeUtc.ToString("O"),
            snapshot.TotalObjects,
            snapshot.TotalBytes,
            snapshot.VersioningStatus,
            snapshot.StorageClassesJson,
            OldestLastModifiedUtc = snapshot.OldestLastModifiedUtc?.ToString("O"),
            NewestLastModifiedUtc = snapshot.NewestLastModifiedUtc?.ToString("O")
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
                SnapshotTimeUtc = DateTime.Parse((string)r.SnapshotTimeUtc),
                TotalObjects = (long)r.TotalObjects,
                TotalBytes = (long)r.TotalBytes,
                VersioningStatus = (string?)r.VersioningStatus,
                StorageClassesJson = (string)(r.StorageClassesJson ?? "{}"),
                OldestLastModifiedUtc = r.OldestLastModifiedUtc != null ? DateTime.Parse((string)r.OldestLastModifiedUtc) : null,
                NewestLastModifiedUtc = r.NewestLastModifiedUtc != null ? DateTime.Parse((string)r.NewestLastModifiedUtc) : null
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
            LastUpdatedAtUtc = checkpoint.LastUpdatedAtUtc.ToString("O")
        };

        await conn.ExecuteAsync(new CommandDefinition(sql, p, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task<ScanCheckpoint?> GetCheckpointAsync(string scanRunId, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = "SELECT CheckpointJson FROM Checkpoints WHERE ScanRunId = @ScanRunId";
        var json = await conn.QuerySingleOrDefaultAsync<string>(new CommandDefinition(sql, new { ScanRunId = scanRunId }, cancellationToken: cancellationToken)).ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(json) ? null : JsonSerializer.Deserialize<ScanCheckpoint>(json);
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
            StartTimeUtc = DateTime.Parse((string)r.StartTimeUtc),
            EndTimeUtc = r.EndTimeUtc != null ? DateTime.Parse((string)r.EndTimeUtc) : null,
            Status = (RunStatus)(int)r.Status,
            Mode = (ComparisonMode)(int)r.Mode,
            ReplicationGracePeriodMinutes = (int)r.ReplicationGracePeriodMinutes,
            GracePeriodCutoffUtc = r.GracePeriodCutoffUtc != null ? DateTime.Parse((string)r.GracePeriodCutoffUtc) : null,
            IsScanWindowClipped = ((long)r.IsScanWindowClipped) == 1,
            ScanWindowNotes = (string)(r.ScanWindowNotes ?? string.Empty),
            LastEvaluatedKey = (string)(r.LastEvaluatedKey ?? string.Empty),
            IsResumedFromCheckpoint = ((long)r.IsResumedFromCheckpoint) == 1,
            ResumedFromKey = (string?)r.ResumedFromKey,
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
