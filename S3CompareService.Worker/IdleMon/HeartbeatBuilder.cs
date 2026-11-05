using System.Globalization;

namespace S3CompareService.Worker.IdleMon;

/// <summary>What the heartbeat is built from. All of it comes from the reporter's own collection loop.</summary>
public sealed class HeartbeatInput
{
    /// <summary>Gets or sets the time of the heartbeat.</summary>
    public DateTimeOffset Now { get; init; }

    /// <summary>Gets or sets the last time the database was read successfully by the collection loop, or null if never.</summary>
    public DateTimeOffset? LastCollectionOk { get; init; }

    /// <summary>Gets or sets the reason the last collection failed, or null when it worked.</summary>
    public string? DatabaseError { get; init; }

    /// <summary>Gets or sets when the service started (the reference for "never collected").</summary>
    public DateTimeOffset StartedAt { get; init; }

    /// <summary>Gets or sets the time the scheduler loop last completed an evaluation, or null when unknown (no scheduler).</summary>
    public DateTimeOffset? SchedulerLastEvaluation { get; init; }

    /// <summary>Gets or sets the scans without progress.</summary>
    public IReadOnlyList<StuckScan> StuckScans { get; init; } = [];

    /// <summary>Gets or sets the age of the oldest message in the spool, or null when the spool is empty.</summary>
    public TimeSpan? Backlog { get; init; }
}

/// <summary>Builds the heartbeat: ok, or degraded with the worst problem named and "(+N more)" for the rest. Pure.</summary>
public static class HeartbeatBuilder
{
    /// <summary>The scheduler loop normally runs every 10 s; after this long without a completed evaluation the heartbeat is degraded.</summary>
    public static readonly TimeSpan SchedulerStallLimit = TimeSpan.FromMinutes(5);

    /// <summary>Gets the time without a successful collection after which the heartbeat says so (3 metrics intervals, at least 10 minutes).</summary>
    public static TimeSpan CollectionStallLimit(IdleMonOptions options)
    {
        var limit = TimeSpan.FromSeconds(options.MetricsIntervalSeconds * 3.0);
        return limit < TimeSpan.FromMinutes(10) ? TimeSpan.FromMinutes(10) : limit;
    }

    /// <summary>Builds the heartbeat for a running service.</summary>
    public static IdleMonMessage Build(HeartbeatInput input, IdleMonOptions options)
    {
        var problems = new List<string>();

        // 1. The database cannot be read (the worst: nothing can be reported).
        if (input.DatabaseError is not null)
            problems.Add($"Database cannot be read: {input.DatabaseError}");
        else if (input.Now - (input.LastCollectionOk ?? input.StartedAt) >= CollectionStallLimit(options))
            problems.Add($"Collector: no result for {(int)(input.Now - (input.LastCollectionOk ?? input.StartedAt)).TotalMinutes} min");

        // 2. The scheduler loop is not evaluating (scheduled scans would not start).
        if (input.SchedulerLastEvaluation is { } scheduler && input.Now - scheduler >= SchedulerStallLimit)
            problems.Add($"Scheduler: no evaluation for {(int)(input.Now - scheduler).TotalMinutes} min");

        // 3. Stuck scans.
        problems.AddRange(input.StuckScans.Select(s => $"Scan of {s.JobName} has made no progress for {s.Minutes} min"));

        string? message = null;
        if (problems.Count > 0)
        {
            message = problems[0] + (problems.Count > 1 ? $" (+{problems.Count - 1} more)" : "");
            if (message.Length > 1024) message = message[..1024];
        }

        return Create(input.Now, problems.Count > 0 ? "degraded" : "ok", message, options, input.Backlog);
    }

    /// <summary>Builds the heartbeat sent on a clean shutdown.</summary>
    public static IdleMonMessage Stopping(DateTimeOffset now, IdleMonOptions options, TimeSpan? backlog) =>
        Create(now, "stopping", "S3CompareService is stopping.", options, backlog);

    private static IdleMonMessage Create(DateTimeOffset now, string status, string? message, IdleMonOptions options, TimeSpan? backlog)
    {
        var tags = new Dictionary<string, string>
        {
            ["dataIntervalSeconds"] = options.MetricsIntervalSeconds.ToString(CultureInfo.InvariantCulture),
        };
        if (backlog is { } age)
            tags["backlogSeconds"] = ((long)Math.Clamp(age.TotalSeconds, 0, 2592000)).ToString(CultureInfo.InvariantCulture);
        return IdleMonMessage.Heartbeat(now, options.HeartbeatIntervalSeconds, status, message, tags);
    }
}
