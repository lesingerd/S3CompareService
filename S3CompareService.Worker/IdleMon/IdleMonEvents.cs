using System.Globalization;
using S3CompareService.Core.Models;

namespace S3CompareService.Worker.IdleMon;

/// <summary>
/// The service's hooks for IdleMon events. The scan coordinator, crash recovery and the job API call these; they never throw and
/// never wait, so reporting can never affect a scan. When reporting is off the no-op implementation is registered.
/// </summary>
public interface IIdleMonEvents
{
    /// <summary>A run completed.</summary>
    void RunCompleted(ComparisonJob job, ScanRun run);

    /// <summary>A run failed.</summary>
    void RunFailed(ComparisonJob job, ScanRun run, Exception error);

    /// <summary>A run was cancelled by a user.</summary>
    void RunCancelled(ComparisonJob job, ScanRun run);

    /// <summary>At startup, an interrupted run was found and resumed (<paramref name="resumed"/>) or marked interrupted.</summary>
    void RunRecovered(ScanRun run, bool resumed);

    /// <summary>A job was created, updated, deleted, enabled or disabled. Carries names only, never credentials.</summary>
    void JobChanged(string action, ComparisonJob job);

    /// <summary>The service started.</summary>
    void ServiceStarted(int? jobCount);
}

/// <summary>Does nothing. Registered when IdleMon reporting is off.</summary>
public sealed class NoOpIdleMonEvents : IIdleMonEvents
{
    /// <summary>The shared instance.</summary>
    public static readonly NoOpIdleMonEvents Instance = new();

    /// <inheritdoc />
    public void RunCompleted(ComparisonJob job, ScanRun run) { }

    /// <inheritdoc />
    public void RunFailed(ComparisonJob job, ScanRun run, Exception error) { }

    /// <inheritdoc />
    public void RunCancelled(ComparisonJob job, ScanRun run) { }

    /// <inheritdoc />
    public void RunRecovered(ScanRun run, bool resumed) { }

    /// <inheritdoc />
    public void JobChanged(string action, ComparisonJob job) { }

    /// <inheritdoc />
    public void ServiceStarted(int? jobCount) { }
}

/// <summary>Builds the catalog's events and hands them to the reporter. Every method swallows every exception.</summary>
public sealed class IdleMonEvents(IIdleMonReporter reporter, IdleMonOptions options, TimeProvider time, string version) : IIdleMonEvents
{
    /// <inheritdoc />
    public void RunCompleted(ComparisonJob job, ScanRun run) => Guard(() =>
    {
        var message = $"Comparison {job.Name} completed: {run.ObjectsCompared.ToString("N0", CultureInfo.InvariantCulture)} objects, "
            + $"{run.FilteredDifferencesCount.ToString("N0", CultureInfo.InvariantCulture)} confirmed disparities, "
            + $"{run.InFlightDifferencesCount.ToString("N0", CultureInfo.InvariantCulture)} in flight, {FormatDuration(run.DurationSeconds)}";
        var fields = new Dictionary<string, object?>
        {
            ["runId"] = run.Id,
            ["objectsCompared"] = run.ObjectsCompared,
            ["matched"] = run.MatchedCount,
            ["confirmed"] = run.FilteredDifferencesCount,
            ["inFlight"] = run.InFlightDifferencesCount,
            ["missingInB"] = run.MissingInBucketBCount,
            ["missingInA"] = run.MissingInBucketACount,
            ["sizeMismatch"] = run.SizeMismatchCount,
            ["etagMismatch"] = run.ETagMismatchCount,
            ["contentMismatch"] = run.ContentMismatchCount,
            ["sequenceGaps"] = run.SequenceGapCount,
            ["durationSeconds"] = run.DurationSeconds,
        };
        Send("info", "scan", message, fields, IdleMonTags.ForJob(job, options));
    });

    /// <inheritdoc />
    public void RunFailed(ComparisonJob job, ScanRun run, Exception error) => Guard(() =>
    {
        var reason = string.IsNullOrWhiteSpace(run.ErrorMessage) ? error.Message : run.ErrorMessage;
        Send("error", "scan", IdleMonLimits.Truncate($"Comparison {job.Name} failed: {reason}", 8000),
            new Dictionary<string, object?> { ["runId"] = run.Id, ["errorType"] = error.GetType().Name, ["objectsCompared"] = run.ObjectsCompared },
            IdleMonTags.ForJob(job, options));
    });

    /// <inheritdoc />
    public void RunCancelled(ComparisonJob job, ScanRun run) => Guard(() =>
        Send("warning", "scan", $"Comparison {job.Name} was cancelled by a user",
            new Dictionary<string, object?> { ["runId"] = run.Id, ["objectsCompared"] = run.ObjectsCompared },
            IdleMonTags.ForJob(job, options)));

    /// <inheritdoc />
    public void RunRecovered(ScanRun run, bool resumed) => Guard(() =>
        Send("warning", "recovery",
            resumed
                ? $"Interrupted run of {run.JobName} was found at startup and resumed from its checkpoint"
                : $"Interrupted run of {run.JobName} was found at startup and marked interrupted (no checkpoint to resume from)",
            new Dictionary<string, object?> { ["runId"] = run.Id, ["resumed"] = resumed, ["objectsCompared"] = run.ObjectsCompared },
            IdleMonTags.ForName(run.JobName, run.JobId)));

    /// <inheritdoc />
    public void JobChanged(string action, ComparisonJob job) => Guard(() =>
        Send("info", "config", $"Job {job.Name} {action}",
            new Dictionary<string, object?> { ["action"] = action, ["enabled"] = job.IsEnabled, ["schedule"] = JobCadence.Describe(job), ["mode"] = job.Mode.ToString() },
            IdleMonTags.ForName(job.Name, job.Id)));

    /// <inheritdoc />
    public void ServiceStarted(int? jobCount) => Guard(() =>
        Send("info", "startup", jobCount is { } n ? $"S3CompareService started: version {version}, {n} job(s)" : $"S3CompareService started: version {version}",
            jobCount is { } count
                ? new Dictionary<string, object?> { ["version"] = version, ["jobs"] = count }
                : new Dictionary<string, object?> { ["version"] = version },
            null));

    private void Send(string severity, string category, string message, Dictionary<string, object?> fields, Dictionary<string, string>? tags) =>
        reporter.Submit(IdleMonMessage.Event(time.GetUtcNow(), severity, category, message, fields, tags));

    private static void Guard(Action action)
    {
        try
        {
            action();
        }
        catch
        {
            // Reporting must never hurt a scan.
        }
    }

    /// <summary>Formats seconds as hh:mm:ss (hours may exceed 24).</summary>
    public static string FormatDuration(double seconds)
    {
        var span = TimeSpan.FromSeconds(Math.Max(0, double.IsFinite(seconds) ? seconds : 0));
        return string.Create(CultureInfo.InvariantCulture, $"{(long)span.TotalHours:00}:{span.Minutes:00}:{span.Seconds:00}");
    }
}
