using System.Data.Common;
using Dapper;

namespace S3CompareService.Core.Storage;

/// <summary>
/// The deduplicated difference store (table <c>DifferenceRecords</c>).
/// <para>
/// Every run created since schema version 5 has a <c>DiffSeq</c> from a counter that never goes back. A record belongs to every
/// run of its job whose DiffSeq lies in [FirstSeq, LastSeq]. When a run finds a difference that a record already describes
/// exactly (same key, type, details, sizes, ETags, modification times, storage classes and in-flight flag) and that record was
/// still current at the previous existing run of the job, the run only raises LastSeq, so a difference that persists across
/// scans is stored once.
/// </para>
/// <para>
/// Deleting runs (by hand or by retention) never rewrites records: a record whose interval no longer contains the DiffSeq of
/// any existing run is garbage and is removed by <see cref="CollectGarbageAsync"/>. A record that still contains one is kept
/// as it is, so deleting the first, the last or a middle run of a series leaves every other run's view unchanged. Intervals
/// may span DiffSeqs of deleted runs or of other jobs' runs; that is harmless because membership is always checked against the
/// runs of the same job that exist.
/// </para>
/// </summary>
public static class DifferenceStore
{
    /// <summary>Where one run's differences are stored.</summary>
    public sealed record RunStorage(string RunId, string JobId, long? DiffSeq, string? GracePeriodCutoffUtc)
    {
        public bool IsDeduplicated => DiffSeq.HasValue;
    }

    /// <summary>The storage of a run, or null when the run does not exist (its leftover rows are then read the legacy way).</summary>
    public static async Task<RunStorage?> GetRunStorageAsync(DbConnection conn, DbTransaction? trans, string runId, CancellationToken ct)
    {
        var row = await conn.QuerySingleOrDefaultAsync(new CommandDefinition(
            "SELECT JobId, DiffSeq, GracePeriodCutoffUtc FROM ScanRuns WHERE Id = @Id;", new { Id = runId }, transaction: trans, cancellationToken: ct)).ConfigureAwait(false);
        if (row == null) return null;
        var d = (IDictionary<string, object?>)row;
        long? seq = d["DiffSeq"] is { } s and not DBNull ? Convert.ToInt64(s) : null;
        return new RunStorage(runId, Convert.ToString(d["JobId"]) ?? string.Empty, seq, d["GracePeriodCutoffUtc"] as string);
    }

    /// <summary>The DiffSeq of the newest existing run of the job before <paramref name="seq"/>, or 0 when there is none.</summary>
    public static async Task<long> PreviousSeqAsync(DbConnection conn, DbTransaction? trans, string jobId, long seq, CancellationToken ct)
        => await conn.ExecuteScalarAsync<long?>(new CommandDefinition(
            "SELECT MAX(DiffSeq) FROM ScanRuns WHERE JobId = @JobId AND DiffSeq < @Seq;", new { JobId = jobId, Seq = seq },
            transaction: trans, cancellationToken: ct)).ConfigureAwait(false) ?? 0;

    /// <summary>
    /// Removes run <paramref name="seq"/> from the records of its job whose Key is in (<paramref name="after"/>, <paramref name="upper"/>]
    /// (null bounds are open): a record only this run had is deleted; a record other runs share is shortened, or split in two
    /// when the run sits in the middle of its interval. Returns the number of records the run no longer has.
    /// </summary>
    public static async Task<int> RemoveRunAsync(DbConnection conn, DbTransaction trans, string jobId, long seq, string? after, string? upper, CancellationToken ct)
    {
        string range = (after != null ? " AND Key > @After" : string.Empty) + (upper != null ? " AND Key <= @Upper" : string.Empty);
        string member = "JobId = @JobId AND FirstSeq <= @Seq AND LastSeq >= @Seq" + range;
        var p = new { JobId = jobId, Seq = seq, After = after, Upper = upper };

        int affected = await conn.ExecuteScalarAsync<int>(new CommandDefinition($"SELECT COUNT(*) FROM DifferenceRecords WHERE {member};", p, transaction: trans, cancellationToken: ct)).ConfigureAwait(false);
        if (affected == 0) return 0;

        // 1. The part after the run (later runs keep it) becomes its own record.
        await conn.ExecuteAsync(new CommandDefinition($@"
            INSERT INTO DifferenceRecords (JobId, FirstSeq, LastSeq, FirstScanRunId, Key, Type, Details, BucketASize, BucketBSize,
                BucketAETag, BucketBETag, BucketALastModified, BucketBLastModified, BucketAStorageClass, BucketBStorageClass,
                DetectedAtUtc, IsWithinGracePeriod, GracePeriodCutoffUtc)
            SELECT JobId, @Seq + 1, LastSeq, FirstScanRunId, Key, Type, Details, BucketASize, BucketBSize,
                BucketAETag, BucketBETag, BucketALastModified, BucketBLastModified, BucketAStorageClass, BucketBStorageClass,
                DetectedAtUtc, IsWithinGracePeriod, GracePeriodCutoffUtc
            FROM DifferenceRecords WHERE {member} AND LastSeq > @Seq;", p, transaction: trans, cancellationToken: ct)).ConfigureAwait(false);
        // 2. The part before the run stays.
        await conn.ExecuteAsync(new CommandDefinition($"UPDATE DifferenceRecords SET LastSeq = @Seq - 1 WHERE {member} AND FirstSeq < @Seq;", p, transaction: trans, cancellationToken: ct)).ConfigureAwait(false);
        // 3. What is left started with this run.
        await conn.ExecuteAsync(new CommandDefinition($"DELETE FROM DifferenceRecords WHERE {member};", p, transaction: trans, cancellationToken: ct)).ConfigureAwait(false);
        return affected;
    }

    /// <summary>
    /// Deletes records that belong to no existing run (their runs were deleted), in batches. Safe while scans run: every
    /// DELETE re-checks that no run of the job has a DiffSeq inside the record's interval. Returns the number of records deleted.
    /// </summary>
    public static async Task<long> CollectGarbageAsync(SqliteConnectionFactory factory, int batchSize, Func<CancellationToken, Task> pause, CancellationToken ct)
    {
        List<string> jobs;
        await using (var conn = await factory.CreateOpenConnectionAsync(ct).ConfigureAwait(false))
        {
            jobs = (await conn.QueryAsync<string>(new CommandDefinition("SELECT DISTINCT JobId FROM DifferenceRecords;", cancellationToken: ct)).ConfigureAwait(false)).ToList();
        }

        const string guard = "NOT EXISTS (SELECT 1 FROM ScanRuns r WHERE r.JobId = d.JobId AND r.DiffSeq BETWEEN d.FirstSeq AND d.LastSeq)";
        long total = 0;
        foreach (var job in jobs)
        {
            List<long> seqs;
            await using (var conn = await factory.CreateOpenConnectionAsync(ct).ConfigureAwait(false))
            {
                seqs = (await conn.QueryAsync<long>(new CommandDefinition(
                    "SELECT DiffSeq FROM ScanRuns WHERE JobId = @JobId AND DiffSeq IS NOT NULL ORDER BY DiffSeq;", new { JobId = job }, cancellationToken: ct)).ConfigureAwait(false)).ToList();
            }

            // A record is garbage exactly when its whole interval lies in a gap between two existing runs (or before the first,
            // or after the last): FirstSeq in (lower, upper) and LastSeq < upper. One index range per gap.
            long lower = long.MinValue;
            for (int i = 0; i <= seqs.Count; i++)
            {
                long? upper = i < seqs.Count ? seqs[i] : null;
                string where = "d.JobId = @JobId AND d.FirstSeq > @Lower" + (upper.HasValue ? " AND d.FirstSeq < @Upper AND d.LastSeq < @Upper" : string.Empty) + " AND " + guard;
                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    int n;
                    await using (var conn = await factory.CreateOpenConnectionAsync(ct).ConfigureAwait(false))
                    {
                        n = await conn.ExecuteAsync(new CommandDefinition(
                            $"DELETE FROM DifferenceRecords WHERE Id IN (SELECT d.Id FROM DifferenceRecords d WHERE {where} LIMIT @Batch);",
                            new { JobId = job, Lower = lower, Upper = upper, Batch = batchSize }, cancellationToken: ct)).ConfigureAwait(false);
                    }

                    total += n;
                    if (n < batchSize) break;
                    await pause(ct).ConfigureAwait(false);
                }

                if (upper.HasValue) lower = upper.Value;
            }
        }

        return total;
    }
}
