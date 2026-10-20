using System.Data;
using System.Text;
using Dapper;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Storage;

public interface IDifferenceRepository
{
    Task InsertBatchAsync(IEnumerable<RecordedDifference> differences, CancellationToken cancellationToken = default);
    Task<List<RecordedDifference>> GetDifferencesAsync(
        string scanRunId,
        DifferenceType? typeFilter = null,
        bool? withinGracePeriodFilter = null,
        string? searchKey = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default);

    Task<int> GetDifferenceCountAsync(
        string scanRunId,
        DifferenceType? typeFilter = null,
        bool? withinGracePeriodFilter = null,
        string? searchKey = null,
        CancellationToken cancellationToken = default);

    Task<List<RecordedDifference>> GetAllForRunAsync(
        string scanRunId,
        bool? withinGracePeriodFilter = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams all differences of a run (optionally excluding in-flight ones) without buffering them in memory.
    /// The underlying connection stays open until enumeration completes or is disposed.
    /// </summary>
    IAsyncEnumerable<RecordedDifference> StreamAllForRunAsync(
        string scanRunId,
        bool? withinGracePeriodFilter = null,
        CancellationToken cancellationToken = default);
}

public class DifferenceRepository : IDifferenceRepository
{
    private readonly SqliteConnectionFactory _factory;

    public DifferenceRepository(SqliteConnectionFactory factory)
    {
        _factory = factory;
    }

    public async Task InsertBatchAsync(IEnumerable<RecordedDifference> differences, CancellationToken cancellationToken = default)
    {
        var list = differences as IList<RecordedDifference> ?? differences.ToList();
        if (list.Count == 0) return;

        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var trans = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);

        string sql = @"
            INSERT INTO RecordedDifferences (
                ScanRunId, Key, Type, Details, BucketASize, BucketBSize,
                BucketAETag, BucketBETag, BucketALastModified, BucketBLastModified,
                BucketAStorageClass, BucketBStorageClass, DetectedAtUtc,
                IsWithinGracePeriod, GracePeriodCutoffUtc
            ) VALUES (
                @ScanRunId, @Key, @Type, @Details, @BucketASize, @BucketBSize,
                @BucketAETag, @BucketBETag, @BucketALastModified, @BucketBLastModified,
                @BucketAStorageClass, @BucketBStorageClass, @DetectedAtUtc,
                @IsWithinGracePeriod, @GracePeriodCutoffUtc
            );
        ";

        var parameters = list.Select(d => new
        {
            d.ScanRunId,
            d.Key,
            Type = (int)d.Type,
            d.Details,
            d.BucketASize,
            d.BucketBSize,
            d.BucketAETag,
            d.BucketBETag,
            BucketALastModified = DbDateTime.ToDb(d.BucketALastModified),
            BucketBLastModified = DbDateTime.ToDb(d.BucketBLastModified),
            d.BucketAStorageClass,
            d.BucketBStorageClass,
            DetectedAtUtc = DbDateTime.ToDb(d.DetectedAtUtc),
            IsWithinGracePeriod = d.IsWithinGracePeriod ? 1 : 0,
            GracePeriodCutoffUtc = DbDateTime.ToDb(d.GracePeriodCutoffUtc)
        });

        await conn.ExecuteAsync(new CommandDefinition(sql, parameters, transaction: trans, cancellationToken: cancellationToken)).ConfigureAwait(false);
        await trans.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<RecordedDifference>> GetDifferencesAsync(
        string scanRunId,
        DifferenceType? typeFilter = null,
        bool? withinGracePeriodFilter = null,
        string? searchKey = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        var (whereSql, dp) = BuildFilter(scanRunId, typeFilter, withinGracePeriodFilter, searchKey);
        dp.Add("Skip", skip);
        dp.Add("Take", take);

        string sql = $@"
            SELECT * FROM RecordedDifferences
            {whereSql}
            ORDER BY Id ASC
            LIMIT @Take OFFSET @Skip;
        ";

        var rows = await conn.QueryAsync(new CommandDefinition(sql, dp, cancellationToken: cancellationToken)).ConfigureAwait(false);
        var list = new List<RecordedDifference>();
        foreach (var r in rows)
        {
            list.Add(MapRow(r));
        }
        return list;
    }

    public async Task<int> GetDifferenceCountAsync(
        string scanRunId,
        DifferenceType? typeFilter = null,
        bool? withinGracePeriodFilter = null,
        string? searchKey = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        var (whereSql, dp) = BuildFilter(scanRunId, typeFilter, withinGracePeriodFilter, searchKey);
        string sql = $"SELECT COUNT(*) FROM RecordedDifferences {whereSql};";

        return await conn.ExecuteScalarAsync<int>(new CommandDefinition(sql, dp, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task<List<RecordedDifference>> GetAllForRunAsync(
        string scanRunId,
        bool? withinGracePeriodFilter = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        var (whereSql, dp) = BuildFilter(scanRunId, null, withinGracePeriodFilter, null);
        string sql = $"SELECT * FROM RecordedDifferences {whereSql} ORDER BY Id ASC;";

        var rows = await conn.QueryAsync(new CommandDefinition(sql, dp, cancellationToken: cancellationToken)).ConfigureAwait(false);
        var list = new List<RecordedDifference>();
        foreach (var r in rows)
        {
            list.Add(MapRow(r));
        }
        return list;
    }

    public async IAsyncEnumerable<RecordedDifference> StreamAllForRunAsync(
        string scanRunId,
        bool? withinGracePeriodFilter = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        var (whereSql, dp) = BuildFilter(scanRunId, null, withinGracePeriodFilter, null);
        string sql = $"SELECT * FROM RecordedDifferences {whereSql} ORDER BY Id ASC;";

        await foreach (var r in conn.QueryUnbufferedAsync(sql, dp).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return MapRow(r);
        }
    }

    private static string EscapeLike(string value)
        => value.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");

    private static (string WhereSql, DynamicParameters Params) BuildFilter(
        string scanRunId,
        DifferenceType? typeFilter,
        bool? withinGracePeriodFilter,
        string? searchKey)
    {
        var sb = new StringBuilder("WHERE ScanRunId = @ScanRunId");
        var dp = new DynamicParameters();
        dp.Add("ScanRunId", scanRunId);

        if (typeFilter.HasValue)
        {
            sb.Append(" AND Type = @Type");
            dp.Add("Type", (int)typeFilter.Value);
        }

        if (withinGracePeriodFilter.HasValue)
        {
            sb.Append(" AND IsWithinGracePeriod = @IsWithinGracePeriod");
            dp.Add("IsWithinGracePeriod", withinGracePeriodFilter.Value ? 1 : 0);
        }

        if (!string.IsNullOrWhiteSpace(searchKey))
        {
            sb.Append(" AND Key LIKE @SearchKey ESCAPE '\\'");
            dp.Add("SearchKey", "%" + EscapeLike(searchKey.Trim()) + "%");
        }

        return (sb.ToString(), dp);
    }

    private static RecordedDifference MapRow(dynamic r)
    {
        return new RecordedDifference
        {
            Id = (long)r.Id,
            ScanRunId = (string)r.ScanRunId,
            Key = (string)r.Key,
            Type = (DifferenceType)(int)r.Type,
            Details = (string)(r.Details ?? string.Empty),
            BucketASize = r.BucketASize != null ? (long?)r.BucketASize : null,
            BucketBSize = r.BucketBSize != null ? (long?)r.BucketBSize : null,
            BucketAETag = (string?)r.BucketAETag,
            BucketBETag = (string?)r.BucketBETag,
            BucketALastModified = DbDateTime.ParseNullable((object?)r.BucketALastModified),
            BucketBLastModified = DbDateTime.ParseNullable((object?)r.BucketBLastModified),
            BucketAStorageClass = (string?)r.BucketAStorageClass,
            BucketBStorageClass = (string?)r.BucketBStorageClass,
            DetectedAtUtc = DbDateTime.Parse((object?)r.DetectedAtUtc),
            IsWithinGracePeriod = ((long)r.IsWithinGracePeriod) == 1,
            GracePeriodCutoffUtc = DbDateTime.ParseNullable((object?)r.GracePeriodCutoffUtc)
        };
    }
}
