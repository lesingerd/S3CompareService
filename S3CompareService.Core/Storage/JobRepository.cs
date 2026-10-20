using System.Data;
using System.Text.Json;
using Dapper;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Storage;

public interface IJobRepository
{
    Task<List<ComparisonJob>> GetAllAsync(CancellationToken cancellationToken = default);
    Task<ComparisonJob?> GetByIdAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts or updates the job's user-editable configuration. For an existing row the server-owned state
    /// (LastRunTimeUtc, NextRunTimeUtc, LastRunStatus and the AlertConfig LastAlertSentUtc / LastDailyDigestSentUtc
    /// timestamps) is preserved, and any secret equal to <see cref="SecretMask.Mask"/> keeps its stored value.
    /// The passed instance is updated in place with the merged values. Use <see cref="UpdateRunScheduleStateAsync"/>,
    /// <see cref="UpdateNextRunTimeAsync"/> and <see cref="UpdateAlertTimestampsAsync"/> to change server-owned state.
    /// </summary>
    Task SaveAsync(ComparisonJob job, CancellationToken cancellationToken = default);
    Task DeleteAsync(string id, CancellationToken cancellationToken = default);
    Task UpdateRunScheduleStateAsync(string id, DateTime lastRunTimeUtc, DateTime? nextRunTimeUtc, RunStatus status, CancellationToken cancellationToken = default);

    /// <summary>Targeted update of NextRunTimeUtc only (null clears it).</summary>
    Task UpdateNextRunTimeAsync(string id, DateTime? nextRunTimeUtc, CancellationToken cancellationToken = default);

    /// <summary>
    /// Targeted update of the alert bookkeeping timestamps inside AlertConfig_Json. A null argument leaves that
    /// timestamp unchanged. Atomic read-modify-write, so it never touches user-edited settings.
    /// </summary>
    Task UpdateAlertTimestampsAsync(string id, DateTime? lastAlertSentUtc, DateTime? lastDailyDigestSentUtc, CancellationToken cancellationToken = default);
}

public class JobRepository : IJobRepository
{
    private readonly SqliteConnectionFactory _factory;

    public JobRepository(SqliteConnectionFactory factory)
    {
        _factory = factory;
    }

    public async Task<List<ComparisonJob>> GetAllAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = "SELECT * FROM Jobs ORDER BY CreatedAtUtc DESC";
        var rows = await conn.QueryAsync(new CommandDefinition(sql, cancellationToken: cancellationToken)).ConfigureAwait(false);

        var list = new List<ComparisonJob>();
        foreach (var r in rows)
        {
            list.Add(MapRowToJob(r));
        }
        return list;
    }

    public async Task<ComparisonJob?> GetByIdAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = "SELECT * FROM Jobs WHERE Id = @Id";
        var row = await conn.QuerySingleOrDefaultAsync(new CommandDefinition(sql, new { Id = id }, cancellationToken: cancellationToken)).ConfigureAwait(false);
        return row != null ? MapRowToJob(row) : null;
    }

    public async Task SaveAsync(ComparisonJob job, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        // Serializable == BEGIN IMMEDIATE: the read-merge-write below is atomic w.r.t. other writers.
        await using var trans = await conn.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);

        var existingRow = await conn.QuerySingleOrDefaultAsync(new CommandDefinition(
            "SELECT * FROM Jobs WHERE Id = @Id", new { job.Id }, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);

        job.UpdatedAtUtc = DateTime.UtcNow;

        if (existingRow != null)
        {
            ComparisonJob existing = MapRowToJob(existingRow);

            SecretMask.MergeMaskedSecrets(job, existing);

            // Server-owned state is never overwritten by a configuration save.
            job.LastRunTimeUtc = existing.LastRunTimeUtc;
            job.NextRunTimeUtc = existing.NextRunTimeUtc;
            job.LastRunStatus = existing.LastRunStatus;
            job.AlertConfig.LastAlertSentUtc = Latest(job.AlertConfig.LastAlertSentUtc, existing.AlertConfig.LastAlertSentUtc);
            job.AlertConfig.LastDailyDigestSentUtc = Latest(job.AlertConfig.LastDailyDigestSentUtc, existing.AlertConfig.LastDailyDigestSentUtc);
            job.CreatedAtUtc = existing.CreatedAtUtc;
        }
        else
        {
            SecretMask.MergeMaskedSecrets(job, null);
        }

        string sql = @"
            INSERT INTO Jobs (
                Id, Name, Description, BucketA_Json, BucketB_Json, Mode, ScheduleType,
                IntervalMinutes, CronExpression, IsEnabled, ReplicationGracePeriodMinutes,
                DetectSequenceGaps, MaxSequenceGapThreshold, AlertConfig_Json, CreatedAtUtc, UpdatedAtUtc,
                LastRunTimeUtc, NextRunTimeUtc, LastRunStatus
            ) VALUES (
                @Id, @Name, @Description, @BucketA_Json, @BucketB_Json, @Mode, @ScheduleType,
                @IntervalMinutes, @CronExpression, @IsEnabled, @ReplicationGracePeriodMinutes,
                @DetectSequenceGaps, @MaxSequenceGapThreshold, @AlertConfig_Json, @CreatedAtUtc, @UpdatedAtUtc,
                @LastRunTimeUtc, @NextRunTimeUtc, @LastRunStatus
            )
            ON CONFLICT(Id) DO UPDATE SET
                Name = excluded.Name,
                Description = excluded.Description,
                BucketA_Json = excluded.BucketA_Json,
                BucketB_Json = excluded.BucketB_Json,
                Mode = excluded.Mode,
                ScheduleType = excluded.ScheduleType,
                IntervalMinutes = excluded.IntervalMinutes,
                CronExpression = excluded.CronExpression,
                IsEnabled = excluded.IsEnabled,
                ReplicationGracePeriodMinutes = excluded.ReplicationGracePeriodMinutes,
                DetectSequenceGaps = excluded.DetectSequenceGaps,
                MaxSequenceGapThreshold = excluded.MaxSequenceGapThreshold,
                AlertConfig_Json = excluded.AlertConfig_Json,
                UpdatedAtUtc = excluded.UpdatedAtUtc;
        ";

        var parameters = new
        {
            job.Id,
            job.Name,
            job.Description,
            BucketA_Json = SerializeBucket(job.BucketA),
            BucketB_Json = SerializeBucket(job.BucketB),
            Mode = (int)job.Mode,
            ScheduleType = (int)job.ScheduleType,
            job.IntervalMinutes,
            job.CronExpression,
            IsEnabled = job.IsEnabled ? 1 : 0,
            job.ReplicationGracePeriodMinutes,
            DetectSequenceGaps = job.DetectSequenceGaps ? 1 : 0,
            job.MaxSequenceGapThreshold,
            AlertConfig_Json = SerializeAlertConfig(job.AlertConfig),
            CreatedAtUtc = DbDateTime.ToDb(job.CreatedAtUtc),
            UpdatedAtUtc = DbDateTime.ToDb(job.UpdatedAtUtc),
            LastRunTimeUtc = DbDateTime.ToDb(job.LastRunTimeUtc),
            NextRunTimeUtc = DbDateTime.ToDb(job.NextRunTimeUtc),
            LastRunStatus = job.LastRunStatus.HasValue ? (int?)job.LastRunStatus.Value : null
        };

        await conn.ExecuteAsync(new CommandDefinition(sql, parameters, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);
        await trans.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = "DELETE FROM Jobs WHERE Id = @Id";
        await conn.ExecuteAsync(new CommandDefinition(sql, new { Id = id }, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task UpdateRunScheduleStateAsync(string id, DateTime lastRunTimeUtc, DateTime? nextRunTimeUtc, RunStatus status, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        string sql = @"
            UPDATE Jobs
            SET LastRunTimeUtc = @LastRunTimeUtc,
                NextRunTimeUtc = @NextRunTimeUtc,
                LastRunStatus = @LastRunStatus,
                UpdatedAtUtc = @UpdatedAtUtc
            WHERE Id = @Id
        ";

        var parameters = new
        {
            Id = id,
            LastRunTimeUtc = DbDateTime.ToDb(lastRunTimeUtc),
            NextRunTimeUtc = DbDateTime.ToDb(nextRunTimeUtc),
            LastRunStatus = (int)status,
            UpdatedAtUtc = DbDateTime.ToDb(DateTime.UtcNow)
        };

        await conn.ExecuteAsync(new CommandDefinition(sql, parameters, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task UpdateNextRunTimeAsync(string id, DateTime? nextRunTimeUtc, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await conn.ExecuteAsync(new CommandDefinition(
            "UPDATE Jobs SET NextRunTimeUtc = @NextRunTimeUtc WHERE Id = @Id",
            new { Id = id, NextRunTimeUtc = DbDateTime.ToDb(nextRunTimeUtc) },
            cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task UpdateAlertTimestampsAsync(string id, DateTime? lastAlertSentUtc, DateTime? lastDailyDigestSentUtc, CancellationToken cancellationToken = default)
    {
        if (!lastAlertSentUtc.HasValue && !lastDailyDigestSentUtc.HasValue) return;

        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var trans = await conn.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);

        var json = await conn.QuerySingleOrDefaultAsync<string?>(new CommandDefinition(
            "SELECT AlertConfig_Json FROM Jobs WHERE Id = @Id", new { Id = id }, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);

        // The stored JSON may contain DPAPI-protected strings; they are carried through untouched
        // (the deserialized model is written straight back without decrypting).
        var cfg = string.IsNullOrWhiteSpace(json)
            ? new JobAlertConfig()
            : JsonSerializer.Deserialize<JobAlertConfig>(json) ?? new JobAlertConfig();

        if (lastAlertSentUtc.HasValue) cfg.LastAlertSentUtc = DbDateTime.ToUtc(lastAlertSentUtc.Value);
        if (lastDailyDigestSentUtc.HasValue) cfg.LastDailyDigestSentUtc = DbDateTime.ToUtc(lastDailyDigestSentUtc.Value);

        await conn.ExecuteAsync(new CommandDefinition(
            "UPDATE Jobs SET AlertConfig_Json = @Json WHERE Id = @Id",
            new { Id = id, Json = JsonSerializer.Serialize(cfg) },
            transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);

        await trans.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static DateTime? Latest(DateTime? a, DateTime? b)
    {
        if (!a.HasValue) return b;
        if (!b.HasValue) return a;
        return a.Value >= b.Value ? a : b;
    }

    private static string SerializeBucket(S3BucketConfig cfg)
    {
        var copy = SecretMask.Clone(cfg);
        SecretProtector.Transform(copy, SecretProtector.Protect);
        return JsonSerializer.Serialize(copy);
    }

    private static string SerializeAlertConfig(JobAlertConfig cfg)
    {
        var copy = SecretMask.Clone(cfg);
        copy.LastAlertSentUtc = DbDateTime.ToUtc(copy.LastAlertSentUtc);
        copy.LastDailyDigestSentUtc = DbDateTime.ToUtc(copy.LastDailyDigestSentUtc);
        SecretProtector.Transform(copy, SecretProtector.Protect);
        return JsonSerializer.Serialize(copy);
    }

    private static string SafeUnprotect(string? stored)
    {
        try { return SecretProtector.Unprotect(stored); }
        catch (Exception ex) when (ex is System.Security.Cryptography.CryptographicException or FormatException or PlatformNotSupportedException)
        {
            return string.Empty; // protected on another machine / unreadable: treat as unset
        }
    }

    private static ComparisonJob MapRowToJob(dynamic r)
    {
        var job = new ComparisonJob
        {
            Id = (string)r.Id,
            Name = (string)r.Name,
            Description = (string)(r.Description ?? string.Empty),
            Mode = (ComparisonMode)(int)r.Mode,
            ScheduleType = (JobScheduleType)(int)r.ScheduleType,
            IntervalMinutes = (int)r.IntervalMinutes,
            CronExpression = (string)(r.CronExpression ?? string.Empty),
            IsEnabled = ((long)r.IsEnabled) == 1,
            ReplicationGracePeriodMinutes = (int)r.ReplicationGracePeriodMinutes,
            DetectSequenceGaps = ((long)r.DetectSequenceGaps) == 1,
            MaxSequenceGapThreshold = (int)r.MaxSequenceGapThreshold,
            CreatedAtUtc = DbDateTime.Parse((object?)r.CreatedAtUtc),
            UpdatedAtUtc = DbDateTime.Parse((object?)r.UpdatedAtUtc),
            LastRunTimeUtc = DbDateTime.ParseNullable((object?)r.LastRunTimeUtc),
            NextRunTimeUtc = DbDateTime.ParseNullable((object?)r.NextRunTimeUtc),
            LastRunStatus = r.LastRunStatus != null ? (RunStatus?)(int)r.LastRunStatus : null
        };

        if (r.BucketA_Json != null)
        {
            job.BucketA = JsonSerializer.Deserialize<S3BucketConfig>((string)r.BucketA_Json) ?? new();
            SecretProtector.Transform(job.BucketA, SafeUnprotect);
        }
        if (r.BucketB_Json != null)
        {
            job.BucketB = JsonSerializer.Deserialize<S3BucketConfig>((string)r.BucketB_Json) ?? new();
            SecretProtector.Transform(job.BucketB, SafeUnprotect);
        }

        if (r is IDictionary<string, object> dict && dict.TryGetValue("AlertConfig_Json", out var alertJson) && alertJson is string jsonStr && !string.IsNullOrWhiteSpace(jsonStr))
        {
            job.AlertConfig = JsonSerializer.Deserialize<JobAlertConfig>(jsonStr) ?? new();
            job.AlertConfig.LastAlertSentUtc = DbDateTime.ToUtc(job.AlertConfig.LastAlertSentUtc);
            job.AlertConfig.LastDailyDigestSentUtc = DbDateTime.ToUtc(job.AlertConfig.LastDailyDigestSentUtc);
            SecretProtector.Transform(job.AlertConfig, SafeUnprotect);
        }

        return job;
    }
}
