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

    public static async Task ExportToCsvAsync(
        TextWriter writer,
        ScanRun run,
        IEnumerable<RecordedDifference> differences,
        bool includeGracePeriodItems = true)
    {
        await writer.WriteLineAsync("# S3 Bucket Comparison Continuous Audit Report").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Generated (UTC)         : {DateTime.UtcNow:O}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Run ID                  : {run.Id}").ConfigureAwait(false);
        await writer.WriteLineAsync($"# Job Name                : {run.JobName}").ConfigureAwait(false);
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
        await writer.WriteLineAsync($"# Breakdown               : MissingInB={run.MissingInBucketBCount:N0}, MissingInA={run.MissingInBucketACount:N0}, SizeMismatch={run.SizeMismatchCount:N0}, ETagMismatch={run.ETagMismatchCount:N0}, ContentMismatch={run.ContentMismatchCount:N0}, SequenceGaps={run.SequenceGapCount:N0}").ConfigureAwait(false);
        await writer.WriteLineAsync().ConfigureAwait(false);

        await writer.WriteLineAsync("Id,Key,Type,ScanWindowTag,IsWithinGracePeriod,Details,BucketASizeBytes,BucketBSizeBytes,BucketAETag,BucketBETag,BucketALastModifiedUtc,BucketBLastModifiedUtc,BucketAStorageClass,BucketBStorageClass,MetadataSummary,DetectedAtUtc").ConfigureAwait(false);

        string escapeCsv(string? val) => $"\"{val?.Replace("\"", "\"\"") ?? ""}\"";

        foreach (var diff in differences)
        {
            if (!includeGracePeriodItems && diff.IsWithinGracePeriod)
                continue;

            string line = $"{diff.Id},{escapeCsv(diff.Key)},{diff.Type},{diff.ScanWindowTag},{diff.IsWithinGracePeriod},{escapeCsv(diff.Details)},{diff.BucketASize?.ToString() ?? ""},{diff.BucketBSize?.ToString() ?? ""},{escapeCsv(diff.BucketAETag)},{escapeCsv(diff.BucketBETag)},{diff.BucketALastModified:O},{diff.BucketBLastModified:O},{escapeCsv(diff.BucketAStorageClass)},{escapeCsv(diff.BucketBStorageClass)},{escapeCsv(diff.MetadataSummary)},{diff.DetectedAtUtc:O}";
            await writer.WriteLineAsync(line).ConfigureAwait(false);
        }

        await writer.FlushAsync().ConfigureAwait(false);
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
}
