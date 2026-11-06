using System.Data.Common;
using Dapper;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Storage;

/// <summary>The <c>S3Compare:Retention</c> settings.</summary>
public sealed class RetentionOptions
{
    /// <summary>Default for <see cref="KeepRunsPerJob"/>.</summary>
    public const int DefaultKeepRunsPerJob = 500;

    /// <summary>Default for <see cref="KeepDays"/>.</summary>
    public const int DefaultKeepDays = 180;

    /// <summary>Default for <see cref="AuditLogDays"/>.</summary>
    public const int DefaultAuditLogDays = 90;

    /// <summary>The newest N runs of every job are kept (any status). 0 means no limit by count.</summary>
    public int KeepRunsPerJob { get; set; } = DefaultKeepRunsPerJob;

    /// <summary>Runs that started more than this many days ago are deleted. 0 means no limit by age.</summary>
    public int KeepDays { get; set; } = DefaultKeepDays;

    /// <summary>Audit log rows older than this many days are deleted. 0 means keep them for ever.</summary>
    public int AuditLogDays { get; set; } = DefaultAuditLogDays;

    /// <summary>Rows deleted per statement (one transaction each); the database is released between batches.</summary>
    public int BatchSize { get; set; } = 2000;

    /// <summary>Pause between batches, so scans and the API get the write lock.</summary>
    public TimeSpan BatchPause { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// An existing database that is not yet in incremental auto_vacuum mode is converted (one VACUUM) only when its file is
    /// at most this many megabytes. 0 disables the conversion. A larger file is left alone and a warning explains how to do it.
    /// </summary>
    public int AutoVacuumConvertMaxMegabytes { get; set; } = 128;

    /// <summary>Replaces invalid (negative) values by their defaults.</summary>
    public RetentionOptions Normalized()
    {
        if (KeepRunsPerJob < 0) KeepRunsPerJob = DefaultKeepRunsPerJob;
        if (KeepDays < 0) KeepDays = DefaultKeepDays;
        if (AuditLogDays < 0) AuditLogDays = DefaultAuditLogDays;
        if (BatchSize < 1) BatchSize = 2000;
        if (BatchPause < TimeSpan.Zero) BatchPause = TimeSpan.Zero;
        if (AutoVacuumConvertMaxMegabytes < 0) AutoVacuumConvertMaxMegabytes = 0;
        return this;
    }
}

/// <summary>What one retention pass did.</summary>
public sealed record RetentionResult
{
    public int RunsDeleted { get; init; }
    public long DifferencesDeleted { get; init; }
    public long SnapshotsDeleted { get; init; }
    public long AuditLogsDeleted { get; init; }

    /// <summary>True when the age rules were skipped because the clock looks far ahead of the newest data.</summary>
    public bool AgeRulesSkippedClockAhead { get; init; }

    /// <summary>"Truncated", "Passive" or "Skipped"/"Failed": how the WAL checkpoint went.</summary>
    public string Checkpoint { get; init; } = "";

    public long PagesVacuumed { get; init; }
}

/// <summary>
/// Keeps the database bounded: deletes old runs (with their differences and snapshots) and old audit log rows in small batches,
/// then checkpoints the WAL and returns free pages to the file. It never touches the newest completed run of a job (IdleMon
/// reporting reads it), a run that can still be resumed (Pending/Running/Paused/Interrupted) or a run a checkpoint refers to,
/// and never the newest run of a job. Safe to call while scans are running.
/// </summary>
public sealed class RetentionManager
{
    private readonly SqliteConnectionFactory _factory;

    public RetentionManager(SqliteConnectionFactory factory)
    {
        _factory = factory;
    }

    /// <summary>One retention pass. Throws on database errors (the hosted service catches them); honours cancellation between batches.</summary>
    public async Task<RetentionResult> RunOnceAsync(RetentionOptions options, DateTime nowUtc, CancellationToken cancellationToken = default)
    {
        options = options.Normalized();
        nowUtc = DbDateTime.ToUtc(nowUtc);

        // Clock-ahead guard: if "now" is further from the newest run than the age limit, the clock was probably set far ahead;
        // every row would look old. The count rule still applies, the age rule waits for the clock to be right.
        bool skipAge = false;
        if (options.KeepDays > 0)
        {
            var newest = await ScalarAsync<string?>("SELECT MAX(StartTimeUtc) FROM ScanRuns", null, cancellationToken).ConfigureAwait(false);
            if (newest != null && nowUtc - DbDateTime.Parse(newest) > TimeSpan.FromDays(options.KeepDays)) skipAge = true;
        }

        var ids = await SelectDeletableRunIdsAsync(options, nowUtc, skipAge, cancellationToken).ConfigureAwait(false);
        long diffs = 0, snaps = 0;
        int runs = 0;
        foreach (var id in ids)
        {
            cancellationToken.ThrowIfCancellationRequested();
            diffs += await DeleteInBatchesAsync("RecordedDifferences", "ScanRunId", id, options, cancellationToken).ConfigureAwait(false);
            snaps += await DeleteInBatchesAsync("BucketMetadataSnapshots", "ScanRunId", id, options, cancellationToken).ConfigureAwait(false);

            // The run row goes last: a pass that stops half way leaves a run without differences, never differences without a run.
            // Re-check the protections in the DELETE itself in case a scan started or a checkpoint appeared since the selection.
            await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            int removed = await conn.ExecuteAsync(new CommandDefinition(
                @"DELETE FROM ScanRuns WHERE Id = @Id
                    AND Status NOT IN (@Pending, @Running, @Paused, @Interrupted)
                    AND Id NOT IN (SELECT ScanRunId FROM Checkpoints);",
                new { Id = id, Pending = (int)RunStatus.Pending, Running = (int)RunStatus.Running, Paused = (int)RunStatus.Paused, Interrupted = (int)RunStatus.Interrupted },
                cancellationToken: cancellationToken)).ConfigureAwait(false);
            runs += removed;
            await PauseAsync(options, cancellationToken).ConfigureAwait(false);
        }

        long logs = 0;
        if (options.AuditLogDays > 0)
        {
            bool skipAudit = false;
            var newestLog = await ScalarAsync<string?>("SELECT MAX(TimestampUtc) FROM ServiceAuditLogs", null, cancellationToken).ConfigureAwait(false);
            if (newestLog != null && nowUtc - DbDateTime.Parse(newestLog) > TimeSpan.FromDays(options.AuditLogDays)) skipAudit = true;
            if (skipAudit) skipAge = true;
            else logs = await PruneAuditLogsAsync(options, nowUtc, cancellationToken).ConfigureAwait(false);
        }

        var checkpoint = await CheckpointWalAsync(cancellationToken).ConfigureAwait(false);
        long pages = await IncrementalVacuumAsync(options, cancellationToken).ConfigureAwait(false);

        return new RetentionResult
        {
            RunsDeleted = runs,
            DifferencesDeleted = diffs,
            SnapshotsDeleted = snaps,
            AuditLogsDeleted = logs,
            AgeRulesSkippedClockAhead = skipAge,
            Checkpoint = checkpoint,
            PagesVacuumed = pages
        };
    }

    private async Task<List<string>> SelectDeletableRunIdsAsync(RetentionOptions o, DateTime nowUtc, bool skipAge, CancellationToken ct)
    {
        string cutoff = o.KeepDays > 0 && !skipAge ? DbDateTime.ToDb(nowUtc.AddDays(-o.KeepDays)) : "";
        if (o.KeepRunsPerJob == 0 && cutoff.Length == 0) return [];

        // rn = 1 is the newest run of the job (never deleted). The newest Completed run is excluded explicitly because
        // it is not always the newest run (a later Failed run does not make it expendable).
        const string sql = @"
            WITH ranked AS (
                SELECT Id, Status, StartTimeUtc,
                       ROW_NUMBER() OVER (PARTITION BY JobId ORDER BY StartTimeUtc DESC, Id DESC) AS rn
                FROM ScanRuns),
            newestCompleted AS (
                SELECT Id FROM (
                    SELECT Id, ROW_NUMBER() OVER (PARTITION BY JobId ORDER BY StartTimeUtc DESC, Id DESC) AS rn
                    FROM ScanRuns WHERE Status = @Completed)
                WHERE rn = 1)
            SELECT Id FROM ranked
            WHERE rn > 1
              AND ((@Keep > 0 AND rn > @Keep) OR (@Cutoff <> '' AND StartTimeUtc < @Cutoff))
              AND Status NOT IN (@Pending, @Running, @Paused, @Interrupted)
              AND Id NOT IN (SELECT Id FROM newestCompleted)
              AND Id NOT IN (SELECT ScanRunId FROM Checkpoints)
            ORDER BY StartTimeUtc ASC;";

        await using var conn = await _factory.CreateOpenConnectionAsync(ct).ConfigureAwait(false);
        var rows = await conn.QueryAsync<string>(new CommandDefinition(sql, new
        {
            Completed = (int)RunStatus.Completed,
            Pending = (int)RunStatus.Pending,
            Running = (int)RunStatus.Running,
            Paused = (int)RunStatus.Paused,
            Interrupted = (int)RunStatus.Interrupted,
            Keep = o.KeepRunsPerJob,
            Cutoff = cutoff
        }, cancellationToken: ct)).ConfigureAwait(false);
        return rows.ToList();
    }

    private async Task<long> DeleteInBatchesAsync(string table, string column, string runId, RetentionOptions o, CancellationToken ct)
    {
        long total = 0;
        string sql = $"DELETE FROM {table} WHERE Id IN (SELECT Id FROM {table} WHERE {column} = @RunId LIMIT @Batch);";
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            int n;
            await using (var conn = await _factory.CreateOpenConnectionAsync(ct).ConfigureAwait(false))
            {
                n = await conn.ExecuteAsync(new CommandDefinition(sql, new { RunId = runId, Batch = o.BatchSize }, cancellationToken: ct)).ConfigureAwait(false);
            }

            total += n;
            if (n < o.BatchSize) return total;
            await PauseAsync(o, ct).ConfigureAwait(false);
        }
    }

    private async Task<long> PruneAuditLogsAsync(RetentionOptions o, DateTime nowUtc, CancellationToken ct)
    {
        string cutoff = DbDateTime.ToDb(nowUtc.AddDays(-o.AuditLogDays));
        const string sql = "DELETE FROM ServiceAuditLogs WHERE Id IN (SELECT Id FROM ServiceAuditLogs WHERE TimestampUtc < @Cutoff LIMIT @Batch);";
        long total = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            int n;
            await using (var conn = await _factory.CreateOpenConnectionAsync(ct).ConfigureAwait(false))
            {
                n = await conn.ExecuteAsync(new CommandDefinition(sql, new { Cutoff = cutoff, Batch = o.BatchSize }, cancellationToken: ct)).ConfigureAwait(false);
            }

            total += n;
            if (n < o.BatchSize) return total;
            await PauseAsync(o, ct).ConfigureAwait(false);
        }
    }

    private static Task PauseAsync(RetentionOptions o, CancellationToken ct) =>
        o.BatchPause > TimeSpan.Zero ? Task.Delay(o.BatchPause, ct) : Task.CompletedTask;

    /// <summary>Truncates the WAL (falls back to a passive checkpoint when readers or writers hold it).</summary>
    private async Task<string> CheckpointWalAsync(CancellationToken ct)
    {
        try
        {
            await using var conn = await _factory.CreateOpenConnectionAsync(ct).ConfigureAwait(false);
            var row = await conn.QuerySingleAsync(new CommandDefinition("PRAGMA wal_checkpoint(TRUNCATE);", cancellationToken: ct)).ConfigureAwait(false);
            if (Convert.ToInt32(((IDictionary<string, object>)row)["busy"]) == 0) return "Truncated";
            await conn.QuerySingleAsync(new CommandDefinition("PRAGMA wal_checkpoint(PASSIVE);", cancellationToken: ct)).ConfigureAwait(false);
            return "Passive";
        }
        catch (Exception ex) when (ex is DbException or InvalidOperationException)
        {
            return "Failed";
        }
    }

    /// <summary>Returns free pages to the file in small steps (only on databases in incremental auto_vacuum mode).</summary>
    private async Task<long> IncrementalVacuumAsync(RetentionOptions o, CancellationToken ct)
    {
        const int stepPages = 5000;
        const int maxSteps = 40; // at most ~200000 pages (~800 MB at 4 KiB) per pass; the rest is done by the next pass
        long total = 0;
        try
        {
            if (await ScalarAsync<int>("PRAGMA auto_vacuum", null, ct).ConfigureAwait(false) != 2) return 0;
            for (int i = 0; i < maxSteps; i++)
            {
                ct.ThrowIfCancellationRequested();
                long free = await ScalarAsync<long>("PRAGMA freelist_count", null, ct).ConfigureAwait(false);
                if (free <= 0) break;
                await using (var conn = await _factory.CreateOpenConnectionAsync(ct).ConfigureAwait(false))
                {
                    await conn.ExecuteAsync(new CommandDefinition($"PRAGMA incremental_vacuum({stepPages});", cancellationToken: ct)).ConfigureAwait(false);
                }

                total += Math.Min(free, stepPages);
                await PauseAsync(o, ct).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is DbException)
        {
            // Busy: the next pass tries again.
        }

        // The vacuum itself writes through the WAL; trim it again.
        if (total > 0) await CheckpointWalAsync(ct).ConfigureAwait(false);
        return total;
    }

    /// <summary>The outcome of <see cref="EnsureIncrementalVacuumAsync"/>.</summary>
    public enum VacuumConversion
    {
        /// <summary>The database already uses incremental auto_vacuum.</summary>
        AlreadyIncremental,

        /// <summary>The database was converted with one VACUUM.</summary>
        Converted,

        /// <summary>The file is larger than the limit (or the conversion is disabled); nothing was done.</summary>
        SkippedTooLarge,

        /// <summary>Not enough free disk space for the temporary copy a VACUUM needs; nothing was done.</summary>
        SkippedNoDiskSpace,

        /// <summary>The VACUUM could not run now (database busy); the next pass tries again.</summary>
        Busy
    }

    /// <summary>
    /// Databases created before this feature are in auto_vacuum NONE mode, which cannot be changed without a VACUUM (a full
    /// rewrite that locks writers). This converts a small database once and leaves a large one alone: for a large one, stop the
    /// service and run <c>PRAGMA auto_vacuum = INCREMENTAL; VACUUM;</c> in the sqlite3 shell. Call it only while no scan runs.
    /// </summary>
    public async Task<VacuumConversion> EnsureIncrementalVacuumAsync(RetentionOptions options, string? databasePath = null, CancellationToken cancellationToken = default)
    {
        if (await ScalarAsync<int>("PRAGMA auto_vacuum", null, cancellationToken).ConfigureAwait(false) == 2) return VacuumConversion.AlreadyIncremental;
        if (options.AutoVacuumConvertMaxMegabytes <= 0) return VacuumConversion.SkippedTooLarge;

        long pages = await ScalarAsync<long>("PRAGMA page_count", null, cancellationToken).ConfigureAwait(false);
        long pageSize = await ScalarAsync<long>("PRAGMA page_size", null, cancellationToken).ConfigureAwait(false);
        long bytes = pages * pageSize;
        if (bytes > options.AutoVacuumConvertMaxMegabytes * 1024L * 1024L) return VacuumConversion.SkippedTooLarge;

        if (databasePath != null)
        {
            try
            {
                var root = Path.GetPathRoot(Path.GetFullPath(databasePath));
                if (!string.IsNullOrEmpty(root) && new DriveInfo(root).AvailableFreeSpace < bytes * 2 + 16 * 1024 * 1024)
                    return VacuumConversion.SkippedNoDiskSpace;
            }
            catch (Exception ex) when (ex is IOException or ArgumentException or UnauthorizedAccessException)
            {
                // Cannot tell: try anyway, a failed VACUUM leaves the database unchanged.
            }
        }

        try
        {
            await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await conn.ExecuteAsync(new CommandDefinition("PRAGMA auto_vacuum = INCREMENTAL; VACUUM;", cancellationToken: cancellationToken)).ConfigureAwait(false);
        }
        catch (DbException)
        {
            return VacuumConversion.Busy;
        }

        return await ScalarAsync<int>("PRAGMA auto_vacuum", null, cancellationToken).ConfigureAwait(false) == 2
            ? VacuumConversion.Converted
            : VacuumConversion.Busy;
    }

    private async Task<T?> ScalarAsync<T>(string sql, object? param, CancellationToken ct)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(ct).ConfigureAwait(false);
        return await conn.ExecuteScalarAsync<T>(new CommandDefinition(sql, param, cancellationToken: ct)).ConfigureAwait(false);
    }
}
