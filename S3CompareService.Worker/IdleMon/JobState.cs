using Cronos;
using S3CompareService.Core.Models;

namespace S3CompareService.Worker.IdleMon;

/// <summary>A scan running now, as far as the reporter needs to know.</summary>
public sealed record ActiveScanInfo(string JobId, string RunId, long ObjectsCompared, double ItemsPerSecond, bool IsPaused);

/// <summary>Where the reporter learns about running scans (the coordinator in production, a fake in tests).</summary>
public interface IActiveScanSource
{
    /// <summary>Gets the scans running now.</summary>
    IReadOnlyList<ActiveScanInfo> GetActiveScans();
}

/// <summary>
/// Everything the pure builders need to know about one job: the job, its recent runs (newest start first) and its running scan.
/// All derived values are computed here once, the same way for metrics, alerts and snapshots.
/// </summary>
public sealed class JobState
{
    /// <summary>Creates the state. <paramref name="runs"/> must be sorted newest start first.</summary>
    public JobState(ComparisonJob job, IReadOnlyList<ScanRun> runs, ActiveScanInfo? activeScan)
    {
        Job = job;
        Runs = runs;
        ActiveScan = activeScan;
        NewestFinished = runs.FirstOrDefault(r => r.Status is RunStatus.Completed or RunStatus.Failed);
        NewestCompleted = runs.FirstOrDefault(r => r.Status == RunStatus.Completed);
        var failures = 0;
        foreach (var run in runs)
        {
            if (run.Status == RunStatus.Completed) break;
            if (run.Status == RunStatus.Failed) failures++;
        }

        ConsecutiveFailures = failures;
    }

    /// <summary>Gets the job.</summary>
    public ComparisonJob Job { get; }

    /// <summary>Gets the recent runs, newest start first.</summary>
    public IReadOnlyList<ScanRun> Runs { get; }

    /// <summary>Gets the running scan of this job, if any.</summary>
    public ActiveScanInfo? ActiveScan { get; }

    /// <summary>Gets the newest run that is Completed or Failed.</summary>
    public ScanRun? NewestFinished { get; }

    /// <summary>Gets the newest Completed run.</summary>
    public ScanRun? NewestCompleted { get; }

    /// <summary>Gets the number of Failed runs since the last Completed run (cancelled and interrupted runs are neither).</summary>
    public int ConsecutiveFailures { get; }

    /// <summary>Gets whether the newest finished run failed.</summary>
    public bool IsFailing => NewestFinished?.Status == RunStatus.Failed;

    /// <summary>Gets the end time of the newest completed run, or null.</summary>
    public DateTime? LastSuccessUtc => NewestCompleted is null ? null : EndOf(NewestCompleted);

    /// <summary>Gets the time a run ended (falls back to start plus duration).</summary>
    public static DateTime EndOf(ScanRun run) => AsUtc(run.EndTimeUtc ?? run.StartTimeUtc.AddSeconds(run.DurationSeconds));

    /// <summary>Treats a database time as UTC.</summary>
    public static DateTime AsUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Utc => value,
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => DateTime.SpecifyKind(value, DateTimeKind.Utc),
    };
}

/// <summary>The cadence of a job: how often a completed run is expected.</summary>
public static class JobCadence
{
    /// <summary>
    /// IntervalMinutes for interval jobs; the gap between the next two cron occurrences (UTC) for cron jobs; 24 h for manual
    /// jobs. Null when it cannot be known (interval of 0 or less, an invalid cron expression).
    /// </summary>
    public static TimeSpan? Of(ComparisonJob job, DateTime nowUtc)
    {
        switch (job.ScheduleType)
        {
            case JobScheduleType.IntervalMinutes:
                return job.IntervalMinutes > 0 ? TimeSpan.FromMinutes(job.IntervalMinutes) : null;
            case JobScheduleType.CronExpression:
                try
                {
                    var expression = CronExpression.Parse(job.CronExpression);
                    var first = expression.GetNextOccurrence(JobState.AsUtc(nowUtc), TimeZoneInfo.Utc);
                    var second = first is null ? null : expression.GetNextOccurrence(first.Value, TimeZoneInfo.Utc);
                    return first is null || second is null ? null : second.Value - first.Value;
                }
                catch (CronFormatException)
                {
                    return null;
                }
            default:
                return TimeSpan.FromHours(24);
        }
    }

    /// <summary>Describes the schedule for the jobs snapshot: "every 60 min", the cron expression, or "manual".</summary>
    public static string Describe(ComparisonJob job) => job.ScheduleType switch
    {
        JobScheduleType.IntervalMinutes => $"every {job.IntervalMinutes} min",
        JobScheduleType.CronExpression => job.CronExpression,
        _ => "manual",
    };
}

/// <summary>The tags every job message carries.</summary>
public static class IdleMonTags
{
    /// <summary>job, jobId, bucketA, bucketB and mode. The bucket tags are A and B when bucket names are not sent.</summary>
    public static Dictionary<string, string> ForJob(ComparisonJob job, IdleMonOptions options) => new()
    {
        ["job"] = IdleMonLimits.Truncate(job.Name ?? "", 256),
        ["jobId"] = IdleMonLimits.Truncate(job.Id, 256),
        ["bucketA"] = BucketName(job.BucketA, "A", options),
        ["bucketB"] = BucketName(job.BucketB, "B", options),
        ["mode"] = job.Mode.ToString(),
    };

    /// <summary>The tags for a message that only knows the run (recovery events, deleted jobs): job and jobId.</summary>
    public static Dictionary<string, string> ForName(string jobName, string jobId) => new()
    {
        ["job"] = IdleMonLimits.Truncate(jobName ?? "", 256),
        ["jobId"] = IdleMonLimits.Truncate(jobId, 256),
    };

    /// <summary>The bucket name, or the placeholder when names are not sent.</summary>
    public static string BucketName(S3BucketConfig bucket, string placeholder, IdleMonOptions options) =>
        options.IncludeBucketNames ? IdleMonLimits.Truncate(bucket.BucketName ?? "", 256) : placeholder;
}
