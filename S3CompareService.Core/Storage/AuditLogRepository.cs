using Dapper;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Storage;

public interface IAuditLogRepository
{
    Task LogAsync(string level, string category, string message, string? details = null, string? scanRunId = null, string? jobId = null, CancellationToken cancellationToken = default);
    Task<List<ServiceAuditLog>> GetRecentLogsAsync(int count = 100, string? levelFilter = null, CancellationToken cancellationToken = default);
}

public class AuditLogRepository : IAuditLogRepository
{
    private readonly SqliteConnectionFactory _factory;

    public AuditLogRepository(SqliteConnectionFactory factory)
    {
        _factory = factory;
    }

    public async Task LogAsync(string level, string category, string message, string? details = null, string? scanRunId = null, string? jobId = null, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            string sql = @"
                INSERT INTO ServiceAuditLogs (TimestampUtc, Level, Category, Message, Details, ScanRunId, JobId)
                VALUES (@TimestampUtc, @Level, @Category, @Message, @Details, @ScanRunId, @JobId);
            ";

            var p = new
            {
                TimestampUtc = DateTime.UtcNow.ToString("O"),
                Level = level.ToUpperInvariant(),
                Category = category,
                Message = message,
                Details = details,
                ScanRunId = scanRunId,
                JobId = jobId
            };

            await conn.ExecuteAsync(new CommandDefinition(sql, p, cancellationToken: cancellationToken)).ConfigureAwait(false);
        }
        catch
        {
            // Logging should never throw
        }
    }

    public async Task<List<ServiceAuditLog>> GetRecentLogsAsync(int count = 100, string? levelFilter = null, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        string sql = string.IsNullOrWhiteSpace(levelFilter)
            ? "SELECT * FROM ServiceAuditLogs ORDER BY TimestampUtc DESC LIMIT @Count;"
            : "SELECT * FROM ServiceAuditLogs WHERE Level = @Level ORDER BY TimestampUtc DESC LIMIT @Count;";

        var rows = await conn.QueryAsync(new CommandDefinition(sql, new { Count = count, Level = levelFilter?.ToUpperInvariant() }, cancellationToken: cancellationToken)).ConfigureAwait(false);
        var list = new List<ServiceAuditLog>();
        foreach (var r in rows)
        {
            list.Add(new ServiceAuditLog
            {
                Id = (long)r.Id,
                TimestampUtc = DateTime.Parse((string)r.TimestampUtc),
                Level = (string)r.Level,
                Category = (string)r.Category,
                Message = (string)r.Message,
                Details = (string?)r.Details,
                ScanRunId = (string?)r.ScanRunId,
                JobId = (string?)r.JobId
            });
        }
        return list;
    }
}
