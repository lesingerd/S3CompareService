using System.Text;
using System.Text.Json;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

public static class ReportExporter
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private static readonly JsonSerializerOptions StreamJsonOptions = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    /// <summary>
    /// CSV field escaping with formula-injection protection: a value starting with = + - @ TAB or CR would be
    /// interpreted as a formula by spreadsheet software, so it is prefixed with an apostrophe.
    /// </summary>
    public static string EscapeCsv(string? val)
    {
        if (string.IsNullOrEmpty(val)) return "\"\"";
        char first = val[0];
        if (first == '=' || first == '+' || first == '-' || first == '@' || first == '\t' || first == '\r')
        {
            val = "'" + val;
        }
        return $"\"{val.Replace("\"", "\"\"")}\"";
    }

    /// <summary>A readable description of a Disparity Explorer filter (for the CSV header and the UI).</summary>
    public static string DescribeViewFilter(DifferenceType? type, bool? withinGracePeriod, string? searchKey)
    {
        string types = type?.ToString() ?? "all types";
        string grace = withinGracePeriod switch
        {
            false => "confirmed disparities only",
            true => "in-flight (grace period) only",
            null => "all raw differences"
        };
        string search = string.IsNullOrWhiteSpace(searchKey) ? "no key search" : $"key contains '{searchKey.Trim()}'";
        return $"{types}; {grace}; {search}";
    }

    private static async Task WriteCsvHeaderAsync(TextWriter writer, ScanRun run, string? viewFilterDescription = null)
    {
        await writer.WriteLineAsync("# S3 Bucket Comparison Continuous Audit Report").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Generated (UTC)         : {DateTime.UtcNow:O}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Run ID                  : {run.Id}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Job Name                : {run.JobName.Replace('\r', ' ').Replace('\n', ' ')}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Scan Status             : {run.Status}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Comparison Mode         : {run.Mode}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Duration                : {run.FormattedDuration} ({run.DurationSeconds:F1}s)").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Objects Evaluated       : {run.ObjectsCompared:N0}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Matched Count           : {run.MatchedCount:N0}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Total Raw Differences   : {run.TotalDifferences:N0}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Confirmed Disparities   : {run.FilteredDifferencesCount:N0}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# In-Flight Replication   : {run.InFlightDifferencesCount:N0}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Grace Period Window     : {(run.ReplicationGracePeriodMinutes > 0 ? $"{run.ReplicationGracePeriodMinutes} minutes (Cutoff: {run.GracePeriodCutoffUtc:O})" : "None (Full scan)")}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Scan Window Clipped     : {run.IsScanWindowClipped} - {run.ScanWindowNotes}").ConfigureAwait(false);
        if (run.DifferencesTruncated)
            await writer.WriteLineAsync("# TRUNCATED              : Yes - the run hit the recorded-differences limit; the counters above are complete but the rows below are only the first ones stored.").ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(viewFilterDescription))
            await writer.WriteLineAsync($"# Explorer View Filter    : {viewFilterDescription.Replace('\r', ' ').Replace('\n', ' ')} (only the rows matching this filter are below)").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Breakdown               : MissingInB={run.MissingInBucketBCount:N0}, MissingInA={run.MissingInBucketACount:N0}, SizeMismatch={run.SizeMismatchCount:N0}, ETagMismatch={run.ETagMismatchCount:N0}, ContentMismatch={run.ContentMismatchCount:N0}, SequenceGaps={run.SequenceGapCount:N0}, ContentCheckErrors={run.ContentCheckErrorCount:N0}").ConfigureAwait(false);
        await writer.WriteLineAsync().ConfigureAwait(false);

        await writer.WriteLineAsync("Id,Key,Type,ScanWindowTag,IsWithinGracePeriod,Details,BucketASizeBytes,BucketBSizeBytes,BucketAETag,BucketBETag,BucketALastModifiedUtc,BucketBLastModifiedUtc,BucketAStorageClass,BucketBStorageClass,MetadataSummary,DetectedAtUtc").ConfigureAwait(false);
    }

    private static string FormatCsvLine(RecordedDifference diff)
        => $"{diff.Id},{EscapeCsv(diff.Key)},{diff.Type},{diff.ScanWindowTag},{diff.IsWithinGracePeriod},{EscapeCsv(diff.Details)},{diff.BucketASize?.ToString() ?? ""},{diff.BucketBSize?.ToString() ?? ""},{EscapeCsv(diff.BucketAETag)},{EscapeCsv(diff.BucketBETag)},{diff.BucketALastModified:O},{diff.BucketBLastModified:O},{EscapeCsv(diff.BucketAStorageClass)},{EscapeCsv(diff.BucketBStorageClass)},{EscapeCsv(diff.MetadataSummary)},{diff.DetectedAtUtc:O}";

    public static async Task ExportToCsvAsync(
        TextWriter writer,
        ScanRun run,
        IEnumerable<RecordedDifference> differences,
        bool includeGracePeriodItems = true)
    {
        await WriteCsvHeaderAsync(writer, run).ConfigureAwait(false);

        foreach (var diff in differences)
        {
            if (!includeGracePeriodItems && diff.IsWithinGracePeriod)
                continue;

            await writer.WriteLineAsync(FormatCsvLine(diff)).ConfigureAwait(false);
        }

        await writer.FlushAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Streaming CSV export: rows are written as they are read from <paramref name="differences"/>
    /// (e.g. <c>IDifferenceRepository.StreamAllForRunAsync</c>), so memory use is constant. The stream is left open.
    /// </summary>
    public static async Task ExportToCsvStreamAsync(
        Stream outputStream,
        ScanRun run,
        IAsyncEnumerable<RecordedDifference> differences,
        bool includeGracePeriodItems = true,
        CancellationToken cancellationToken = default,
        string? viewFilterDescription = null)
    {
        await using var writer = new StreamWriter(outputStream, new UTF8Encoding(false), 16 * 1024, leaveOpen: true);
        await WriteCsvHeaderAsync(writer, run, viewFilterDescription).ConfigureAwait(false);

        await foreach (var diff in differences.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (!includeGracePeriodItems && diff.IsWithinGracePeriod)
                continue;

            await writer.WriteLineAsync(FormatCsvLine(diff)).ConfigureAwait(false);
        }

        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public static async Task ExportToJsonAsync(
        Stream outputStream,
        ScanRun run,
        IEnumerable<RecordedDifference> differences,
        IEnumerable<BucketMetadataSnapshot>? snapshots = null,
        bool includeGracePeriodItems = true)
    {
        var diffList = includeGracePeriodItems
            ? (differences as IList<RecordedDifference> ?? differences.ToList())
            : differences.Where(d => !d.IsWithinGracePeriod).ToList();

        var report = new
        {
            GeneratedAtUtc = DateTime.UtcNow,
            Run = run,
            BucketSnapshots = snapshots ?? Enumerable.Empty<BucketMetadataSnapshot>(),
            FilterApplied = new
            {
                IncludeGracePeriodInFlight = includeGracePeriodItems,
                ReplicationGracePeriodMinutes = run.ReplicationGracePeriodMinutes,
                GracePeriodCutoffUtc = run.GracePeriodCutoffUtc,
                TotalExportedDifferences = diffList.Count
            },
            Differences = diffList
        };

        await JsonSerializer.SerializeAsync(outputStream, report, JsonOptions).ConfigureAwait(false);
    }

    /// <summary>
    /// Streaming JSON export using <see cref="Utf8JsonWriter"/>; differences are written one by one so the
    /// result set is never materialized. Same shape as <see cref="ExportToJsonAsync"/> except that, because the
    /// count is only known at the end, <c>totalExportedDifferences</c> is written after the <c>differences</c> array.
    /// The stream is left open.
    /// <para>The writer writes into a memory buffer, never into <paramref name="outputStream"/> directly: JsonSerializer.Serialize
    /// flushes its writer synchronously, and a synchronous write to an ASP.NET response stream throws ("Synchronous operations are
    /// disallowed"), which cut the HTTP export off mid-response. The buffer is copied to the stream with WriteAsync every 500
    /// differences (or when it passes 1 MB), so memory stays bounded.</para>
    /// </summary>
    public static async Task ExportToJsonStreamAsync(
        Stream outputStream,
        ScanRun run,
        IAsyncEnumerable<RecordedDifference> differences,
        IEnumerable<BucketMetadataSnapshot>? snapshots = null,
        bool includeGracePeriodItems = true,
        CancellationToken cancellationToken = default)
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>(256 * 1024);
        await using var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = false, SkipValidation = false });

        async Task DrainAsync()
        {
            writer.Flush(); // into the memory buffer only
            if (buffer.WrittenCount == 0) return;
            await outputStream.WriteAsync(buffer.WrittenMemory, cancellationToken).ConfigureAwait(false);
            buffer.Clear();
        }

        writer.WriteStartObject();
        writer.WriteString("generatedAtUtc", DateTime.UtcNow);
        writer.WritePropertyName("run");
        JsonSerializer.Serialize(writer, run, StreamJsonOptions);

        writer.WritePropertyName("bucketSnapshots");
        JsonSerializer.Serialize(writer, snapshots ?? Enumerable.Empty<BucketMetadataSnapshot>(), StreamJsonOptions);

        writer.WriteStartObject("filterApplied");
        writer.WriteBoolean("includeGracePeriodInFlight", includeGracePeriodItems);
        writer.WriteNumber("replicationGracePeriodMinutes", run.ReplicationGracePeriodMinutes);
        if (run.GracePeriodCutoffUtc.HasValue) writer.WriteString("gracePeriodCutoffUtc", run.GracePeriodCutoffUtc.Value);
        else writer.WriteNull("gracePeriodCutoffUtc");
        writer.WriteEndObject();

        writer.WriteStartArray("differences");
        await DrainAsync().ConfigureAwait(false);
        long count = 0;
        await foreach (var diff in differences.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (!includeGracePeriodItems && diff.IsWithinGracePeriod)
                continue;

            JsonSerializer.Serialize(writer, diff, StreamJsonOptions);
            count++;

            if (count % 500 == 0 || writer.BytesPending + buffer.WrittenCount > 1024 * 1024)
            {
                await DrainAsync().ConfigureAwait(false);
            }
        }
        writer.WriteEndArray();

        writer.WriteNumber("totalExportedDifferences", count);
        writer.WriteEndObject();
        await DrainAsync().ConfigureAwait(false);
        await outputStream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}
