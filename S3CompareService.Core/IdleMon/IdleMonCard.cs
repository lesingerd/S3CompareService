namespace S3CompareService.Core.IdleMon;

/// <summary>The shape of <c>GET /api/idlemon/status</c> as the UI reads it.</summary>
public sealed class IdleMonStatusDto
{
    public bool Enabled { get; set; }
    public string Mode { get; set; } = "Disabled";
    public string? ConfigProblem { get; set; }
    public DateTime? LastSendAt { get; set; }
    public string? LastError { get; set; }
    public int QueueLength { get; set; }
    public int SpoolFiles { get; set; }
    public long SpoolBytes { get; set; }
    public long Accepted { get; set; }
    public long Rejected { get; set; }
    public long Spooled { get; set; }
    public long Dropped { get; set; }
    public int HeartbeatIntervalSeconds { get; set; } = 60;
    public string SettingsFile { get; set; } = "";
    public string DryRunFile { get; set; } = "";
    public bool SettingsFileExists { get; set; }
    public bool ApiKeyPresent { get; set; }
}

/// <summary>The states the IdleMon card can show.</summary>
public enum IdleMonCardState
{
    /// <summary>The service could not be asked (offline).</summary>
    Unknown,
    /// <summary>Reporting is disabled.</summary>
    Off,
    /// <summary>Messages go to a file instead of the agent.</summary>
    DryRun,
    /// <summary>Sending works: a recent send, no error, nothing spooled.</summary>
    Sending,
    /// <summary>The agent could not be reached; messages wait in the spool.</summary>
    Spooling,
    /// <summary>Enabled but nothing has been sent recently and there is no error yet (for example just started).</summary>
    Waiting,
    /// <summary>The settings are wrong; reporting is off because of it.</summary>
    ConfigProblem,
}

/// <summary>Pure logic behind the IdleMon card of the UI (kept free of WPF so it can be tested).</summary>
public static class IdleMonCard
{
    /// <summary>A send counts as recent within this many heartbeat intervals.</summary>
    public const double RecentFactor = 2;

    /// <summary>Works out the state to show. <paramref name="status"/> is null when the service did not answer.</summary>
    public static IdleMonCardState Derive(IdleMonStatusDto? status, DateTime utcNow)
    {
        if (status is null) return IdleMonCardState.Unknown;
        if (!string.IsNullOrWhiteSpace(status.ConfigProblem)) return IdleMonCardState.ConfigProblem;
        if (!status.Enabled) return IdleMonCardState.Off;
        if (string.Equals(status.Mode, "DryRun", StringComparison.OrdinalIgnoreCase)) return IdleMonCardState.DryRun;
        if (!string.IsNullOrWhiteSpace(status.LastError) || status.SpoolFiles > 0) return IdleMonCardState.Spooling;

        var window = TimeSpan.FromSeconds(Math.Max(10, status.HeartbeatIntervalSeconds) * RecentFactor);
        if (status.LastSendAt is { } last && utcNow - DateTime.SpecifyKind(last, DateTimeKind.Utc) <= window)
            return IdleMonCardState.Sending;
        return IdleMonCardState.Waiting;
    }

    /// <summary>The badge text for a state.</summary>
    public static string Label(IdleMonCardState state) => state switch
    {
        IdleMonCardState.Off => "Off",
        IdleMonCardState.DryRun => "Dry run",
        IdleMonCardState.Sending => "Sending",
        IdleMonCardState.Spooling => "Agent unreachable \u2014 spooling",
        IdleMonCardState.Waiting => "No recent send",
        IdleMonCardState.ConfigProblem => "Config problem",
        _ => "Unknown",
    };

    /// <summary>Short relative time such as "5 s ago", "3 min ago", or "never".</summary>
    public static string Relative(DateTime? utc, DateTime utcNow)
    {
        if (utc is not { } t) return "never";
        var age = utcNow - DateTime.SpecifyKind(t, DateTimeKind.Utc);
        if (age < TimeSpan.Zero) age = TimeSpan.Zero;
        if (age.TotalSeconds < 60) return $"{(int)age.TotalSeconds} s ago";
        if (age.TotalMinutes < 60) return $"{(int)age.TotalMinutes} min ago";
        if (age.TotalHours < 48) return $"{(int)age.TotalHours} h ago";
        return $"{(int)age.TotalDays} d ago";
    }

    /// <summary>Formats a byte count such as "12 KB".</summary>
    public static string Size(long bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => $"{bytes / 1024.0:F0} KB",
        _ => $"{bytes / (1024.0 * 1024.0):F1} MB",
    };
}
