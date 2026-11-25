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
    /// Streams all differences of a run (optionally excluding in-flight ones) without buffering them in memory, in key order
    /// (a parallel scan inserts them out of key order; the (ScanRunId, Key) index serves the sort).
    /// The underlying connection stays open until enumeration completes or is disposed.
    /// </summary>
    IAsyncEnumerable<RecordedDifference> StreamAllForRunAsync(
        string scanRunId,
        bool? withinGracePeriodFilter = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams the differences that match the same filter as <see cref="GetDifferencesAsync"/> (every page of it, in the same
    /// order), without buffering them. Used to export the Disparity Explorer's current view.
    /// </summary>
    IAsyncEnumerable<RecordedDifference> StreamFilteredAsync(
        string scanRunId,
        DifferenceType? typeFilter,
        bool? withinGracePeriodFilter,
        string? searchKey,
        CancellationToken cancellationToken = default);

    /// <summary>How a run's differences are stored (for the audit log), or null when unknown.</summary>
    Task<DifferenceStorageStats?> GetStorageStatsAsync(string scanRunId, CancellationToken cancellationToken = default)
        => Task.FromResult<DifferenceStorageStats?>(null);
}

/// <summary>A run's differences: how many it has, and how many of them it had to store as new records.</summary>
public sealed record DifferenceStorageStats(bool Deduplicated, long InRun, long NewRecords)
{
    public long CarriedOver => InRun - NewRecords;
}


/// <summary>
/// Stores and reads differences. Runs created since schema version 5 use the deduplicated store (see
/// <see cref="DifferenceStore"/>): a difference that repeats unchanged in the next scan of the same job is not stored again.
/// Older runs (DiffSeq NULL) keep their one-row-per-run table <c>RecordedDifferences</c>. Callers see no difference: every
/// method takes a run id and returns that run's differences, with <see cref="RecordedDifference.ScanRunId"/> set to it.
/// In the deduplicated store <see cref="RecordedDifference.DetectedAtUtc"/> is when the difference was first detected.
/// </summary>
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

        foreach (var group in list.GroupBy(d => d.ScanRunId))
        {
            var storage = await DifferenceStore.GetRunStorageAsync(conn, trans, group.Key, cancellationToken).ConfigureAwait(false);
            if (storage?.DiffSeq is long seq)
                await InsertDeduplicatedAsync(conn, trans, storage.JobId, seq, group, cancellationToken).ConfigureAwait(false);
            else
                await InsertLegacyAsync(conn, trans, group, cancellationToken).ConfigureAwait(false);
        }

        await trans.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task InsertLegacyAsync(Microsoft.Data.Sqlite.SqliteConnection conn, System.Data.Common.DbTransaction trans, IEnumerable<RecordedDifference> list, CancellationToken cancellationToken)
    {
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
    }

    // Columns that make two differences "the same" (everything except when it was detected and the run's grace cutoff).
    private const string IdentityMatch = @"Type = $Type AND Details IS $Details
        AND BucketASize IS $BucketASize AND BucketBSize IS $BucketBSize AND BucketAETag IS $BucketAETag AND BucketBETag IS $BucketBETag
        AND BucketALastModified IS $BucketALastModified AND BucketBLastModified IS $BucketBLastModified
        AND BucketAStorageClass IS $BucketAStorageClass AND BucketBStorageClass IS $BucketBStorageClass
        AND IsWithinGracePeriod = $IsWithinGracePeriod";

    /// <summary>
    /// For each difference: if the job has a record describing it exactly that was current at the previous existing run (or is
    /// already this run's), extend it to this run; otherwise add a record for this run only.
    /// </summary>
    private static async Task InsertDeduplicatedAsync(Microsoft.Data.Sqlite.SqliteConnection conn, System.Data.Common.DbTransaction trans, string jobId, long seq, IEnumerable<RecordedDifference> list, CancellationToken ct)
    {
        long prev = await DifferenceStore.PreviousSeqAsync(conn, trans, jobId, seq, ct).ConfigureAwait(false);

        await using var update = conn.CreateCommand();
        update.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)trans;
        update.CommandText = $@"UPDATE DifferenceRecords SET LastSeq = $Seq
            WHERE JobId = $JobId AND Key = $Key AND LastSeq BETWEEN $Prev AND $Seq AND FirstSeq <= $Seq AND {IdentityMatch};";

        await using var insert = conn.CreateCommand();
        insert.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)trans;
        insert.CommandText = @"INSERT INTO DifferenceRecords (JobId, FirstSeq, LastSeq, FirstScanRunId, Key, Type, Details, BucketASize, BucketBSize,
                BucketAETag, BucketBETag, BucketALastModified, BucketBLastModified, BucketAStorageClass, BucketBStorageClass,
                DetectedAtUtc, IsWithinGracePeriod, GracePeriodCutoffUtc)
            VALUES ($JobId, $Seq, $Seq, $RunId, $Key, $Type, $Details, $BucketASize, $BucketBSize,
                $BucketAETag, $BucketBETag, $BucketALastModified, $BucketBLastModified, $BucketAStorageClass, $BucketBStorageClass,
                $DetectedAtUtc, $IsWithinGracePeriod, $GracePeriodCutoffUtc);";

        foreach (var d in list)
        {
            ct.ThrowIfCancellationRequested();
            object V(object? v) => v ?? DBNull.Value;
            var values = new (string Name, object Value)[]
            {
                ("$JobId", jobId), ("$Seq", seq), ("$Prev", prev), ("$RunId", d.ScanRunId), ("$Key", d.Key), ("$Type", (int)d.Type),
                ("$Details", V(d.Details)), ("$BucketASize", V(d.BucketASize)), ("$BucketBSize", V(d.BucketBSize)),
                ("$BucketAETag", V(d.BucketAETag)), ("$BucketBETag", V(d.BucketBETag)),
                ("$BucketALastModified", V(DbDateTime.ToDb(d.BucketALastModified))), ("$BucketBLastModified", V(DbDateTime.ToDb(d.BucketBLastModified))),
                ("$BucketAStorageClass", V(d.BucketAStorageClass)), ("$BucketBStorageClass", V(d.BucketBStorageClass)),
                ("$IsWithinGracePeriod", d.IsWithinGracePeriod ? 1 : 0),
                ("$DetectedAtUtc", V(DbDateTime.ToDb(d.DetectedAtUtc))), ("$GracePeriodCutoffUtc", V(DbDateTime.ToDb(d.GracePeriodCutoffUtc)))
            };

            Bind(update, values);
            int extended = await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (extended > 0) continue;
            Bind(insert, values);
            await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    private static void Bind(Microsoft.Data.Sqlite.SqliteCommand cmd, (string Name, object Value)[] values)
    {
        foreach (var (name, value) in values)
        {
            if (!cmd.CommandText.Contains(name, StringComparison.Ordinal)) continue;
            if (cmd.Parameters.Contains(name)) cmd.Parameters[name].Value = value;
            else cmd.Parameters.AddWithValue(name, value);
        }
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
        var q = await BuildQueryAsync(conn, scanRunId, typeFilter, withinGracePeriodFilter, searchKey, cancellationToken).ConfigureAwait(false);
        q.Params.Add("Skip", skip);
        q.Params.Add("Take", take);

        string sql = $"SELECT * FROM {q.Table} {q.Where} {q.OrderBy} LIMIT @Take OFFSET @Skip;";
        var rows = await conn.QueryAsync(new CommandDefinition(sql, q.Params, cancellationToken: cancellationToken)).ConfigureAwait(false);
        var list = new List<RecordedDifference>();
        foreach (var r in rows) list.Add(q.Map(r));
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
        var q = await BuildQueryAsync(conn, scanRunId, typeFilter, withinGracePeriodFilter, searchKey, cancellationToken).ConfigureAwait(false);
        return await conn.ExecuteScalarAsync<int>(new CommandDefinition($"SELECT COUNT(*) FROM {q.Table} {q.Where};", q.Params, cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    public async Task<List<RecordedDifference>> GetAllForRunAsync(
        string scanRunId,
        bool? withinGracePeriodFilter = null,
        CancellationToken cancellationToken = default)
    {
        var list = new List<RecordedDifference>();
        await foreach (var d in StreamFilteredAsync(scanRunId, null, withinGracePeriodFilter, null, cancellationToken).ConfigureAwait(false))
            list.Add(d);
        return list;
    }

    public IAsyncEnumerable<RecordedDifference> StreamAllForRunAsync(
        string scanRunId,
        bool? withinGracePeriodFilter = null,
        CancellationToken cancellationToken = default)
        => StreamFilteredAsync(scanRunId, null, withinGracePeriodFilter, null, cancellationToken);

    public async IAsyncEnumerable<RecordedDifference> StreamFilteredAsync(
        string scanRunId,
        DifferenceType? typeFilter,
        bool? withinGracePeriodFilter,
        string? searchKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var q = await BuildQueryAsync(conn, scanRunId, typeFilter, withinGracePeriodFilter, searchKey, cancellationToken).ConfigureAwait(false);
        string sql = $"SELECT * FROM {q.Table} {q.Where} {q.OrderBy};";

        await foreach (var r in conn.QueryUnbufferedAsync(sql, q.Params).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return q.Map(r);
        }
    }

    public async Task<DifferenceStorageStats?> GetStorageStatsAsync(string scanRunId, CancellationToken cancellationToken = default)
    {
        await using var conn = await _factory.CreateOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var storage = await DifferenceStore.GetRunStorageAsync(conn, null, scanRunId, cancellationToken).ConfigureAwait(false);
        if (storage == null) return null;
        if (storage.DiffSeq is not long seq)
        {
            long rows = await conn.ExecuteScalarAsync<long>(new CommandDefinition("SELECT COUNT(*) FROM RecordedDifferences WHERE ScanRunId = @Id;", new { Id = scanRunId }, cancellationToken: cancellationToken)).ConfigureAwait(false);
            return new DifferenceStorageStats(false, rows, rows);
        }

        var row = await conn.QuerySingleAsync(new CommandDefinition(
            "SELECT COUNT(*) AS InRun, COALESCE(SUM(FirstSeq = @Seq), 0) AS NewRecords FROM DifferenceRecords WHERE JobId = @JobId AND +FirstSeq <= @Seq AND +LastSeq >= @Seq;",
            new { storage.JobId, Seq = seq }, cancellationToken: cancellationToken)).ConfigureAwait(false);
        return new DifferenceStorageStats(true, (long)row.InRun, (long)row.NewRecords);
    }

    private static string EscapeLike(string value)
        => value.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");

    private sealed record Query(string Table, string Where, string OrderBy, DynamicParameters Params, Func<dynamic, RecordedDifference> Map);

    private static async Task<Query> BuildQueryAsync(
        Microsoft.Data.Sqlite.SqliteConnection conn,
        string scanRunId,
        DifferenceType? typeFilter,
        bool? withinGracePeriodFilter,
        string? searchKey,
        CancellationToken ct)
    {
        var storage = await DifferenceStore.GetRunStorageAsync(conn, null, scanRunId, ct).ConfigureAwait(false);
        var dp = new DynamicParameters();
        StringBuilder sb;
        string table, orderBy;
        Func<dynamic, RecordedDifference> map;

        if (storage?.DiffSeq is long seq)
        {
            table = "DifferenceRecords";
            // The unary + keeps SQLite from using IX_DiffRec_Seq (meant for garbage collection) here: with it the planner sorts
            // the whole run in a temp B-tree; without it a (JobId, [filter,] Key, FirstSeq, LastSeq) index serves the order.
            sb = new StringBuilder("WHERE JobId = @JobId AND +FirstSeq <= @Seq AND +LastSeq >= @Seq");
            dp.Add("JobId", storage.JobId);
            dp.Add("Seq", seq);
            orderBy = "ORDER BY Key ASC, FirstSeq ASC, LastSeq ASC, Id ASC";
            DateTime? cutoff = storage.GracePeriodCutoffUtc != null ? DbDateTime.ParseNullable(storage.GracePeriodCutoffUtc) : null;
            map = r =>
            {
                var d = MapRow(r, scanRunId);
                if (cutoff.HasValue) d.GracePeriodCutoffUtc = cutoff; // the run's own window, not the first detection's
                return d;
            };
        }
        else
        {
            table = "RecordedDifferences";
            sb = new StringBuilder("WHERE ScanRunId = @ScanRunId");
            dp.Add("ScanRunId", scanRunId);
            orderBy = "ORDER BY Key ASC, Id ASC";
            map = r => MapRow(r, null);
        }

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

        return new Query(table, sb.ToString(), orderBy, dp, map);
    }

    private static RecordedDifference MapRow(dynamic r, string? runId)
    {
        return new RecordedDifference
        {
            Id = (long)r.Id,
            ScanRunId = runId ?? (string)r.ScanRunId,
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
