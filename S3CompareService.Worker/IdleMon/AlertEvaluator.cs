using System.Globalization;
using S3CompareService.Core.Notifications;

namespace S3CompareService.Worker.IdleMon;

/// <summary>One alert that is true right now.</summary>
public sealed record AlertCondition(string Key, string Severity, string Title, string? Message, Dictionary<string, object?> Fields, Dictionary<string, string> Tags);

/// <summary>A scan without progress, for the heartbeat.</summary>
public sealed record StuckScan(string JobName, int Minutes);

/// <summary>The result of one evaluation pass.</summary>
public sealed record AlertEvaluation(IReadOnlyList<AlertCondition> Conditions, int OverdueJobs, IReadOnlyList<StuckScan> StuckScans);

/// <summary>Remembers when each running scan last made progress (its ObjectsCompared changed).</summary>
public sealed class ScanProgressTracker
{
    private readonly Dictionary<string, (long Objects, DateTimeOffset ChangedAt)> _seen = new();

    /// <summary>Records the scans seen now; scans that are gone are forgotten. A paused scan counts as progressing (the pause is deliberate).</summary>
    public void Observe(IEnumerable<ActiveScanInfo> scans, DateTimeOffset now)
    {
        var current = new Dictionary<string, (long, DateTimeOffset)>();
        foreach (var scan in scans)
        {
            var changedAt = _seen.TryGetValue(scan.RunId, out var before) && before.Objects == scan.ObjectsCompared && !scan.IsPaused ? before.ChangedAt : now;
            current[scan.RunId] = (scan.ObjectsCompared, changedAt);
        }

        _seen.Clear();
        foreach (var (key, value) in current) _seen[key] = value;
    }

    /// <summary>Gets how long a scan has made no progress, or zero when unknown.</summary>
    public TimeSpan NoProgressFor(string runId, DateTimeOffset now) => _seen.TryGetValue(runId, out var s) ? now - s.ChangedAt : TimeSpan.Zero;
}

/// <summary>Decides which alerts are true, from the job states. Pure.</summary>
public static class AlertEvaluator
{
    /// <summary>Evaluates run-failed, disparities, overdue and scan-stuck for every enabled job. Disabled jobs have no alerts.</summary>
    public static AlertEvaluation Evaluate(IReadOnlyList<JobState> jobs, DateTimeOffset now, IdleMonOptions options, ScanProgressTracker progress)
    {
        var conditions = new List<AlertCondition>();
        var stuck = new List<StuckScan>();
        var overdueCount = 0;

        foreach (var state in jobs.Where(j => j.Job.IsEnabled))
        {
            var job = state.Job;
            var tags = IdleMonTags.ForJob(job, options);

            // run-failed: the newest finished run failed. Resolves when the next run completes.
            if (state.NewestFinished is { Status: Core.Models.RunStatus.Failed } failed)
            {
                conditions.Add(new AlertCondition(
                    $"run-failed/{job.Id}",
                    "error",
                    $"Comparison {job.Name} failed",
                    IdleMonLimits.Truncate(string.IsNullOrWhiteSpace(failed.ErrorMessage) ? "The comparison failed (no error message was recorded)." : failed.ErrorMessage, 1024),
                    new Dictionary<string, object?>
                    {
                        ["runId"] = failed.Id,
                        ["failedAt"] = IdleMonWire.Timestamp(new DateTimeOffset(JobState.EndOf(failed), TimeSpan.Zero)),
                        ["consecutiveFailures"] = state.ConsecutiveFailures,
                    },
                    tags));
            }

            // disparities: the newest completed run breaches the job's own thresholds (whether or not e-mail alerting is on).
            if (state.NewestCompleted is { } completed)
            {
                var reasons = AlertNotificationService.EvaluateDisparityThresholds(job.AlertConfig, completed);
                if (reasons.Count > 0)
                {
                    conditions.Add(new AlertCondition(
                        $"disparities/{job.Id}",
                        completed.MissingInBucketBCount > 0 ? "error" : "warning",
                        $"{completed.FilteredDifferencesCount.ToString(CultureInfo.InvariantCulture)} confirmed disparities in {job.Name}",
                        IdleMonLimits.Truncate(string.Join("; ", reasons), 1024),
                        new Dictionary<string, object?>
                        {
                            ["runId"] = completed.Id,
                            ["confirmed"] = completed.FilteredDifferencesCount,
                            ["inFlight"] = completed.InFlightDifferencesCount,
                            ["missingInB"] = completed.MissingInBucketBCount,
                            ["missingInA"] = completed.MissingInBucketACount,
                            ["sizeMismatch"] = completed.SizeMismatchCount,
                            ["etagMismatch"] = completed.ETagMismatchCount,
                            ["contentMismatch"] = completed.ContentMismatchCount,
                            ["sequenceGaps"] = completed.SequenceGapCount,
                        },
                        tags));
                }
            }

            // overdue
            if (Overdue(state, now, options) is { } age)
            {
                overdueCount++;
                conditions.Add(new AlertCondition(
                    $"overdue/{job.Id}",
                    "warning",
                    $"{job.Name} has not completed a comparison for {FormatAge(age)}",
                    state.LastSuccessUtc is { } last ? $"The last completed comparison ended at {IdleMonWire.Timestamp(new DateTimeOffset(last, TimeSpan.Zero))}." : "No comparison has completed since the job was created.",
                    new Dictionary<string, object?> { ["ageSeconds"] = (long)age.TotalSeconds },
                    tags));
            }

            // scan-stuck
            if (state.ActiveScan is { IsPaused: false } scan)
            {
                var idle = progress.NoProgressFor(scan.RunId, now);
                if (idle >= TimeSpan.FromMinutes(options.StuckScanMinutes))
                {
                    var minutes = (int)idle.TotalMinutes;
                    stuck.Add(new StuckScan(job.Name, minutes));
                    conditions.Add(new AlertCondition(
                        $"scan-stuck/{job.Id}",
                        "warning",
                        $"Scan of {job.Name} has made no progress for {minutes} min",
                        $"The scan has compared {scan.ObjectsCompared:N0} objects and has not advanced.",
                        new Dictionary<string, object?> { ["runId"] = scan.RunId, ["objectsCompared"] = scan.ObjectsCompared, ["minutes"] = minutes },
                        tags));
                }
            }
        }

        return new AlertEvaluation(conditions, overdueCount, stuck);
    }

    /// <summary>
    /// The time without a completed run when the job is overdue, otherwise null. An enabled, scheduled (not manual) job is
    /// overdue when it has had no completed run for OverdueMultiplier x cadence + 15 minutes, measured from the newest
    /// completed run or, if there is none, from the job's creation.
    /// </summary>
    public static TimeSpan? Overdue(JobState state, DateTimeOffset now, IdleMonOptions options)
    {
        var job = state.Job;
        if (!job.IsEnabled || job.ScheduleType == Core.Models.JobScheduleType.Manual) return null;
        if (JobCadence.Of(job, now.UtcDateTime) is not { } cadence) return null;

        var since = state.LastSuccessUtc ?? JobState.AsUtc(job.CreatedAtUtc);
        var age = now.UtcDateTime - since;
        var limit = TimeSpan.FromTicks((long)(cadence.Ticks * options.OverdueMultiplier)) + TimeSpan.FromMinutes(15);
        return age > limit ? age : null;
    }

    /// <summary>Formats an age for a title: "45 min", "3 h 20 min", "2 d 4 h".</summary>
    public static string FormatAge(TimeSpan age)
    {
        if (age < TimeSpan.FromHours(1)) return $"{(int)age.TotalMinutes} min";
        if (age < TimeSpan.FromHours(48)) return $"{(int)age.TotalHours} h {age.Minutes} min";
        return $"{(int)age.TotalDays} d {age.Hours} h";
    }
}

/// <summary>
/// Keeps the set of firing alerts in memory. Every pass it is given the alerts that are true now: it sends firing for all of
/// them (new ones start, known ones are refreshed every metrics interval) and resolved for the ones that are no longer true.
/// After a restart the set is empty, so it never sends resolved for an alert it did not send; currently true conditions are
/// simply sent as firing again.
/// </summary>
public sealed class AlertStateMachine
{
    private readonly Dictionary<string, AlertCondition> _firing = new(StringComparer.Ordinal);

    /// <summary>Gets the keys of the alerts firing now.</summary>
    public IReadOnlyCollection<string> FiringKeys => _firing.Keys;

    /// <summary>Applies the conditions that are true now and returns the messages to send.</summary>
    public List<IdleMonMessage> Apply(IReadOnlyList<AlertCondition> desired, DateTimeOffset now)
    {
        var messages = new List<IdleMonMessage>();
        var desiredKeys = desired.Select(c => c.Key).ToHashSet(StringComparer.Ordinal);

        foreach (var (key, previous) in _firing.ToList())
        {
            if (desiredKeys.Contains(key)) continue;
            messages.Add(IdleMonMessage.AlertResolved(now, key, previous.Tags));
            _firing.Remove(key);
        }

        foreach (var condition in desired)
        {
            messages.Add(IdleMonMessage.AlertFiring(now, condition.Key, condition.Severity, condition.Title, condition.Message, condition.Fields, condition.Tags));
            _firing[condition.Key] = condition;
        }

        return messages;
    }
}
