using S3CompareService.Core.Models;
using S3CompareService.Worker.Services;

namespace S3CompareService.Worker.IdleMon;

/// <summary>Builds the snapshots of the catalog. Pure.</summary>
public static class SnapshotBuilder
{
    /// <summary>The most rows in the jobs snapshot.</summary>
    public const int MaxJobRows = 100;

    /// <summary>The most rows in the top-disparities snapshot.</summary>
    public const int MaxDisparityRows = 50;

    private const int MaxKeyLength = 512;

    private static readonly string[] JobColumnNames =
        ["job", "enabled", "schedule", "mode", "bucketA", "bucketB", "lastRunAt", "lastStatus", "lastSuccessAt", "confirmed", "inFlight", "missingInB", "objects", "nextRunAt", "overdue"];

    private static readonly string[] JobColumnTypes =
        ["string", "bool", "string", "string", "string", "string", "datetime", "string", "datetime", "number", "number", "number", "number", "datetime", "bool"];

    /// <summary>Builds <c>s3c.jobs</c>: no job tag, one row per job (enabled jobs first, then by name), at most 100.</summary>
    public static IdleMonMessage Jobs(IReadOnlyList<JobState> jobs, DateTimeOffset now, IdleMonOptions options)
    {
        var columns = JobColumnNames.Select((name, i) => new IdleMonColumn { Name = name, Type = JobColumnTypes[i] }).ToList();
        var rows = jobs
            .OrderByDescending(j => j.Job.IsEnabled)
            .ThenBy(j => j.Job.Name, StringComparer.OrdinalIgnoreCase)
            .Take(MaxJobRows)
            .Select(state => Row(state, now, options))
            .ToList();
        return IdleMonMessage.Snapshot(now, "s3c.jobs", columns, rows);
    }

    private static object?[] Row(JobState state, DateTimeOffset now, IdleMonOptions options)
    {
        var job = state.Job;
        var newest = state.Runs.FirstOrDefault();
        var completed = state.NewestCompleted;
        return
        [
            IdleMonLimits.Truncate(job.Name ?? "", 256),
            job.IsEnabled,
            IdleMonLimits.Truncate(JobCadence.Describe(job), 256),
            job.Mode.ToString(),
            IdleMonTags.BucketName(job.BucketA, "A", options),
            IdleMonTags.BucketName(job.BucketB, "B", options),
            newest is null ? null : Time(newest.EndTimeUtc ?? newest.StartTimeUtc),
            newest?.Status.ToString(),
            state.LastSuccessUtc is { } last ? Time(last) : null,
            completed?.FilteredDifferencesCount,
            completed?.InFlightDifferencesCount,
            completed?.MissingInBucketBCount,
            completed?.ObjectsCompared,
            NextRun(state, now) is { } next ? Time(next) : null,
            AlertEvaluator.Overdue(state, now, options) is not null,
        ];
    }

    private static DateTime? NextRun(JobState state, DateTimeOffset now)
    {
        var job = state.Job;
        if (!job.IsEnabled || job.ScheduleType == JobScheduleType.Manual) return null;
        try
        {
            return ScanSchedulerService.ComputeSchedule(job, now.UtcDateTime, state.Runs.FirstOrDefault()?.StartTimeUtc).NextRunUtc;
        }
        catch (Exception ex) when (ex is Cronos.CronFormatException or ArgumentException)
        {
            return null;
        }
    }

    private static string Time(DateTime value) => IdleMonWire.Timestamp(new DateTimeOffset(JobState.AsUtc(value), TimeSpan.Zero));

    /// <summary>
    /// Builds <c>s3c.run.top_disparities</c> for one job from up to 50 confirmed differences of its newest completed run. It
    /// contains object keys, so the caller sends it only when IncludeObjectKeys is true. Its timestamp is the run's end time.
    /// </summary>
    public static IdleMonMessage TopDisparities(JobState state, IReadOnlyList<RecordedDifference> differences, IdleMonOptions options)
    {
        var run = state.NewestCompleted ?? throw new ArgumentException("The job has no completed run.", nameof(state));
        List<IdleMonColumn> columns =
        [
            new() { Name = "key", Type = "string" },
            new() { Name = "type", Type = "string" },
            new() { Name = "sizeA", Type = "number", Unit = "bytes" },
            new() { Name = "sizeB", Type = "number", Unit = "bytes" },
            new() { Name = "lastModifiedA", Type = "datetime" },
            new() { Name = "lastModifiedB", Type = "datetime" },
        ];
        var rows = differences.Take(MaxDisparityRows).Select(d => new object?[]
        {
            IdleMonLimits.Truncate(d.Key, MaxKeyLength),
            d.Type.ToString(),
            d.BucketASize,
            d.BucketBSize,
            d.BucketALastModified is { } a ? Time(a) : null,
            d.BucketBLastModified is { } b ? Time(b) : null,
        }).ToList();
        return IdleMonMessage.Snapshot(new DateTimeOffset(JobState.EndOf(run), TimeSpan.Zero), "s3c.run.top_disparities", columns, rows, IdleMonTags.ForJob(state.Job, options));
    }
}
