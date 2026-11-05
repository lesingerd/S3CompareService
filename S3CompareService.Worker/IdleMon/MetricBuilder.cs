using S3CompareService.Core.Models;

namespace S3CompareService.Worker.IdleMon;

/// <summary>Remembers which completed run's counts were already sent for each job, so the first send carries the run's end time and later re-sends the send time.</summary>
public sealed class ResultSendTracker
{
    private readonly Dictionary<string, string> _sentRunByJob = new();

    /// <summary>Gets whether the counts of this run were sent before.</summary>
    public bool WasSent(string jobId, string runId) => _sentRunByJob.TryGetValue(jobId, out var sent) && sent == runId;

    /// <summary>Records that the counts of this run were sent.</summary>
    public void MarkSent(string jobId, string runId) => _sentRunByJob[jobId] = runId;
}

/// <summary>Builds the interval metrics of the catalog. Pure: it reads nothing but its arguments.</summary>
public static class MetricBuilder
{
    /// <summary>The shortest time a completed run's counts keep being re-sent.</summary>
    public static readonly TimeSpan MinResultWindow = TimeSpan.FromMinutes(10);

    /// <summary>Gets how long the counts of the newest completed run of a job keep being re-sent: StaleResultMultiplier cadences, at least 10 minutes.</summary>
    public static TimeSpan ResultWindow(ComparisonJob job, DateTime nowUtc, IdleMonOptions options)
    {
        var cadence = JobCadence.Of(job, nowUtc) ?? TimeSpan.FromHours(24);
        var window = TimeSpan.FromTicks((long)(cadence.Ticks * options.StaleResultMultiplier));
        return window < MinResultWindow ? MinResultWindow : window;
    }

    /// <summary>Builds the per-job and service-level metrics for one collection pass.</summary>
    public static List<IdleMonMessage> Build(IReadOnlyList<JobState> jobs, int activeScans, int overdueJobs, DateTimeOffset now, IdleMonOptions options, ResultSendTracker tracker)
    {
        var list = new List<IdleMonMessage>();
        var enabled = jobs.Where(j => j.Job.IsEnabled).ToList();

        foreach (var state in enabled)
        {
            var job = state.Job;
            var tags = IdleMonTags.ForJob(job, options);

            if (state.LastSuccessUtc is { } lastSuccess)
                list.Add(IdleMonMessage.Metric(now, "s3c.job.last_success.age", Math.Max(0, (now.UtcDateTime - lastSuccess).TotalSeconds), "seconds", tags));
            list.Add(IdleMonMessage.Metric(now, "s3c.job.consecutive_failures", state.ConsecutiveFailures, "count", tags));

            if (state.NewestCompleted is { } run && state.LastSuccessUtc is { } end && now.UtcDateTime - end <= ResultWindow(job, now.UtcDateTime, options))
            {
                var first = !tracker.WasSent(job.Id, run.Id);
                var at = first ? new DateTimeOffset(end < now.UtcDateTime ? end : now.UtcDateTime, TimeSpan.Zero) : now;
                list.AddRange(ResultMetrics(run, at, tags));
                tracker.MarkSent(job.Id, run.Id);
            }

            if (state.ActiveScan is { } scan)
            {
                list.Add(IdleMonMessage.Metric(now, "s3c.scan.objects_compared", scan.ObjectsCompared, "count", tags));
                if (!scan.IsPaused && double.IsFinite(scan.ItemsPerSecond))
                    list.Add(IdleMonMessage.Metric(now, "s3c.scan.objects_per_second", scan.ItemsPerSecond, "per_second", tags));
            }
        }

        list.Add(IdleMonMessage.Metric(now, "s3c.jobs.enabled", enabled.Count, "count"));
        list.Add(IdleMonMessage.Metric(now, "s3c.jobs.failing", enabled.Count(j => j.IsFailing), "count"));
        list.Add(IdleMonMessage.Metric(now, "s3c.jobs.overdue", overdueJobs, "count"));
        list.Add(IdleMonMessage.Metric(now, "s3c.jobs.with_disparities", enabled.Count(j => j.NewestCompleted is { FilteredDifferencesCount: > 0 }), "count"));
        list.Add(IdleMonMessage.Metric(now, "s3c.scans.active", activeScans, "count"));
        return list;
    }

    /// <summary>The counts of one completed run. A failed run is never passed in: its partial counts are unknown, not zero.</summary>
    public static List<IdleMonMessage> ResultMetrics(ScanRun run, DateTimeOffset at, Dictionary<string, string> tags)
    {
        var list = new List<IdleMonMessage>
        {
            IdleMonMessage.Metric(at, "s3c.run.disparities.confirmed", run.FilteredDifferencesCount, "count", tags),
            IdleMonMessage.Metric(at, "s3c.run.disparities.inflight", run.InFlightDifferencesCount, "count", tags),
            IdleMonMessage.Metric(at, "s3c.run.missing_in_b", run.MissingInBucketBCount, "count", tags),
            IdleMonMessage.Metric(at, "s3c.run.missing_in_a", run.MissingInBucketACount, "count", tags),
            IdleMonMessage.Metric(at, "s3c.run.size_mismatch", run.SizeMismatchCount, "count", tags),
            IdleMonMessage.Metric(at, "s3c.run.etag_mismatch", run.ETagMismatchCount, "count", tags),
            IdleMonMessage.Metric(at, "s3c.run.content_mismatch", run.ContentMismatchCount, "count", tags),
            IdleMonMessage.Metric(at, "s3c.run.sequence_gaps", run.SequenceGapCount, "count", tags),
            IdleMonMessage.Metric(at, "s3c.run.objects_compared", run.ObjectsCompared, "count", tags),
        };
        if (run.ObjectsCompared > 0)
            list.Add(IdleMonMessage.Metric(at, "s3c.run.match_rate", Math.Round(run.MatchedCount * 100.0 / run.ObjectsCompared, 4), "percent", tags));
        list.Add(IdleMonMessage.Metric(at, "s3c.run.duration", run.DurationSeconds, "seconds", tags));
        list.Add(IdleMonMessage.Metric(at, "s3c.run.bytes_read", (double)run.BucketABytesRead + run.BucketBBytesRead, "bytes", tags));
        return list;
    }
}
