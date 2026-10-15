using System.Text.Json;
using Dapper;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Storage;

public interface IJobRepository
{
    Task<List<ComparisonJob>> GetAllAsync(CancellationToken cancellationToken = default);
    Task<ComparisonJob?> GetByIdAsync(string id, CancellationToken cancellationToken = default);
    Task SaveAsync(ComparisonJob job, CancellationToken cancellationToken = default);
    Task DeleteAsync(string id, CancellationToken cancellationToken = default);
    Task UpdateRunScheduleStateAsync(string id, DateTime lastRunTimeUtc, DateTime? nextRunTimeUtc, RunStatus status, CancellationToken cancellationToken = default);
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
        job.UpdatedAtUtc = DateTime.UtcNow;

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
                UpdatedAtUtc = excluded.UpdatedAtUtc,
                LastRunTimeUtc = excluded.LastRunTimeUtc,
                NextRunTimeUtc = excluded.NextRunTimeUtc,
                LastRunStatus = excluded.LastRunStatus;
        ";

        var parameters = new
        {
            job.Id,
            job.Name,
            job.Description,
            BucketA_Json = JsonSerializer.Serialize(job.BucketA),
            BucketB_Json = JsonSerializer.Serialize(job.BucketB),
            Mode = (int)job.Mode,
            ScheduleType = (int)job.ScheduleType,
            job.IntervalMinutes,
            job.CronExpression,
            IsEnabled = job.IsEnabled ? 1 : 0,
            job.ReplicationGracePeriodMinutes,
            DetectSequenceGaps = job.DetectSequenceGaps ? 1 : 0,
            job.MaxSequenceGapThreshold,
            AlertConfig_Json = JsonSerializer.Serialize(job.AlertConfig),
            CreatedAtUtc = job.CreatedAtUtc.ToString("O"),
            UpdatedAtUtc = job.UpdatedAtUtc.ToString("O"),
            LastRunTimeUtc = job.LastRunTimeUtc?.ToString("O"),
            NextRunTimeUtc = job.NextRunTimeUtc?.ToString("O"),
            LastRunStatus = job.LastRunStatus.HasValue ? (int?)job.LastRunStatus.Value : null
        };

        await conn.ExecuteAsync(new CommandDefinition(sql, parameters, cancellationToken: cancellationToken)).ConfigureAwait(false);
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
            LastRunTimeUtc = lastRunTimeUtc.ToString("O"),
            NextRunTimeUtc = nextRunTimeUtc?.ToString("O"),
            LastRunStatus = (int)status,
            UpdatedAtUtc = DateTime.UtcNow.ToString("O")
        };

        await conn.ExecuteAsync(new CommandDefinition(sql, parameters, cancellationToken: cancellationToken)).ConfigureAwait(false);
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
            CreatedAtUtc = DateTime.Parse((string)r.CreatedAtUtc),
            UpdatedAtUtc = DateTime.Parse((string)r.UpdatedAtUtc),
            LastRunTimeUtc = r.LastRunTimeUtc != null ? DateTime.Parse((string)r.LastRunTimeUtc) : null,
            NextRunTimeUtc = r.NextRunTimeUtc != null ? DateTime.Parse((string)r.NextRunTimeUtc) : null,
            LastRunStatus = r.LastRunStatus != null ? (RunStatus?)(int)r.LastRunStatus : null
        };

        if (r.BucketA_Json != null)
        {
            job.BucketA = JsonSerializer.Deserialize<S3BucketConfig>((string)r.BucketA_Json) ?? new();
        }
        if (r.BucketB_Json != null)
        {
            job.BucketB = JsonSerializer.Deserialize<S3BucketConfig>((string)r.BucketB_Json) ?? new();
        }

        if (r is IDictionary<string, object> dict && dict.TryGetValue("AlertConfig_Json", out var alertJson) && alertJson is string jsonStr && !string.IsNullOrWhiteSpace(jsonStr))
        {
            job.AlertConfig = JsonSerializer.Deserialize<JobAlertConfig>(jsonStr) ?? new();
        }

        return job;
    }
}
