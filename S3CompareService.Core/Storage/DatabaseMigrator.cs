using Dapper;

namespace S3CompareService.Core.Storage;

public class DatabaseMigrator
{
    private readonly SqliteConnectionFactory _factory;

    public DatabaseMigrator(SqliteConnectionFactory factory)
    {
        _factory = factory;
    }

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        string sql = @"
            -- Jobs Table
            CREATE TABLE IF NOT EXISTS Jobs (
                Id TEXT PRIMARY KEY,
                Name TEXT NOT NULL,
                Description TEXT,
                BucketA_Json TEXT NOT NULL,
                BucketB_Json TEXT NOT NULL,
                Mode INTEGER NOT NULL DEFAULT 0,
                ScheduleType INTEGER NOT NULL DEFAULT 0,
                IntervalMinutes INTEGER NOT NULL DEFAULT 60,
                CronExpression TEXT,
                IsEnabled INTEGER NOT NULL DEFAULT 1,
                ReplicationGracePeriodMinutes INTEGER NOT NULL DEFAULT 15,
                DetectSequenceGaps INTEGER NOT NULL DEFAULT 1,
                MaxSequenceGapThreshold INTEGER NOT NULL DEFAULT 25,
                AlertConfig_Json TEXT,
                CreatedAtUtc TEXT NOT NULL,
                UpdatedAtUtc TEXT NOT NULL,
                LastRunTimeUtc TEXT,
                NextRunTimeUtc TEXT,
                LastRunStatus INTEGER
            );

            -- ScanRuns Table
            CREATE TABLE IF NOT EXISTS ScanRuns (
                Id TEXT PRIMARY KEY,
                JobId TEXT NOT NULL,
                JobName TEXT NOT NULL,
                StartTimeUtc TEXT NOT NULL,
                EndTimeUtc TEXT,
                Status INTEGER NOT NULL DEFAULT 0,
                Mode INTEGER NOT NULL DEFAULT 0,
                ReplicationGracePeriodMinutes INTEGER NOT NULL DEFAULT 15,
                GracePeriodCutoffUtc TEXT,
                IsScanWindowClipped INTEGER NOT NULL DEFAULT 0,
                ScanWindowNotes TEXT,
                LastEvaluatedKey TEXT,
                IsResumedFromCheckpoint INTEGER NOT NULL DEFAULT 0,
                ResumedFromKey TEXT,
                ObjectsCompared INTEGER NOT NULL DEFAULT 0,
                MatchedCount INTEGER NOT NULL DEFAULT 0,
                TotalDifferences INTEGER NOT NULL DEFAULT 0,
                FilteredDifferencesCount INTEGER NOT NULL DEFAULT 0,
                InFlightDifferencesCount INTEGER NOT NULL DEFAULT 0,
                MissingInBucketBCount INTEGER NOT NULL DEFAULT 0,
                MissingInBucketACount INTEGER NOT NULL DEFAULT 0,
                SizeMismatchCount INTEGER NOT NULL DEFAULT 0,
                ETagMismatchCount INTEGER NOT NULL DEFAULT 0,
                ContentMismatchCount INTEGER NOT NULL DEFAULT 0,
                SequenceGapCount INTEGER NOT NULL DEFAULT 0,
                BucketAObjectsRead INTEGER NOT NULL DEFAULT 0,
                BucketBObjectsRead INTEGER NOT NULL DEFAULT 0,
                BucketABytesRead INTEGER NOT NULL DEFAULT 0,
                BucketBBytesRead INTEGER NOT NULL DEFAULT 0,
                DurationSeconds REAL NOT NULL DEFAULT 0,
                ErrorMessage TEXT
            );

            CREATE INDEX IF NOT EXISTS IX_ScanRuns_JobId ON ScanRuns (JobId);
            CREATE INDEX IF NOT EXISTS IX_ScanRuns_StartTimeUtc ON ScanRuns (StartTimeUtc DESC);
            CREATE INDEX IF NOT EXISTS IX_ScanRuns_Status ON ScanRuns (Status);

            -- RecordedDifferences Table
            CREATE TABLE IF NOT EXISTS RecordedDifferences (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                ScanRunId TEXT NOT NULL,
                Key TEXT NOT NULL,
                Type INTEGER NOT NULL,
                Details TEXT,
                BucketASize INTEGER,
                BucketBSize INTEGER,
                BucketAETag TEXT,
                BucketBETag TEXT,
                BucketALastModified TEXT,
                BucketBLastModified TEXT,
                BucketAStorageClass TEXT,
                BucketBStorageClass TEXT,
                DetectedAtUtc TEXT NOT NULL,
                IsWithinGracePeriod INTEGER NOT NULL DEFAULT 0,
                GracePeriodCutoffUtc TEXT
            );

            CREATE INDEX IF NOT EXISTS IX_Differences_ScanRunId ON RecordedDifferences (ScanRunId);
            CREATE INDEX IF NOT EXISTS IX_Differences_ScanRun_Grace ON RecordedDifferences (ScanRunId, IsWithinGracePeriod);
            CREATE INDEX IF NOT EXISTS IX_Differences_ScanRun_Type ON RecordedDifferences (ScanRunId, Type);
            CREATE INDEX IF NOT EXISTS IX_Differences_Key ON RecordedDifferences (Key);

            -- BucketMetadataSnapshots Table
            CREATE TABLE IF NOT EXISTS BucketMetadataSnapshots (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                ScanRunId TEXT NOT NULL,
                BucketSide TEXT NOT NULL,
                BucketName TEXT NOT NULL,
                EndpointUrl TEXT NOT NULL,
                Region TEXT,
                Prefix TEXT,
                SnapshotTimeUtc TEXT NOT NULL,
                TotalObjects INTEGER NOT NULL DEFAULT 0,
                TotalBytes INTEGER NOT NULL DEFAULT 0,
                VersioningStatus TEXT,
                StorageClassesJson TEXT,
                OldestLastModifiedUtc TEXT,
                NewestLastModifiedUtc TEXT
            );

            CREATE INDEX IF NOT EXISTS IX_MetadataSnapshots_ScanRunId ON BucketMetadataSnapshots (ScanRunId);

            -- Checkpoints Table (for crash recovery)
            CREATE TABLE IF NOT EXISTS Checkpoints (
                ScanRunId TEXT PRIMARY KEY,
                JobId TEXT NOT NULL,
                CheckpointJson TEXT NOT NULL,
                LastUpdatedAtUtc TEXT NOT NULL
            );

            -- ServiceAuditLogs Table
            CREATE TABLE IF NOT EXISTS ServiceAuditLogs (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                TimestampUtc TEXT NOT NULL,
                Level TEXT NOT NULL,
                Category TEXT NOT NULL,
                Message TEXT NOT NULL,
                Details TEXT,
                ScanRunId TEXT,
                JobId TEXT
            );

            CREATE INDEX IF NOT EXISTS IX_AuditLogs_Timestamp ON ServiceAuditLogs (TimestampUtc DESC);
            CREATE INDEX IF NOT EXISTS IX_AuditLogs_Level ON ServiceAuditLogs (Level);
        ";

        await conn.ExecuteAsync(new CommandDefinition(sql, cancellationToken: cancellationToken)).ConfigureAwait(false);

        // Incremental column migration for existing databases
        var columns = (await conn.QueryAsync<string>(
            new CommandDefinition("SELECT name FROM pragma_table_info('Jobs')", cancellationToken: cancellationToken)
        ).ConfigureAwait(false)).ToList();

        if (!columns.Contains("AlertConfig_Json", StringComparer.OrdinalIgnoreCase))
        {
            await conn.ExecuteAsync(
                new CommandDefinition("ALTER TABLE Jobs ADD COLUMN AlertConfig_Json TEXT;", cancellationToken: cancellationToken)
            ).ConfigureAwait(false);
        }
    }
}
