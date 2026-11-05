using System.Net;
using System.Text.RegularExpressions;

namespace S3CompareService.Worker.IdleMon;

/// <summary>The <c>IdleMon</c> section of appsettings.json. Everything is off by default.</summary>
public sealed partial class IdleMonOptions
{
    /// <summary>Name of the configuration section.</summary>
    public const string SectionName = "IdleMon";

    /// <summary>The application name on the wire (<c>source.app</c>).</summary>
    public const string AppName = "S3CompareService";

    /// <summary>Gets or sets whether the reporter runs. When false nothing is sent, nothing is written.</summary>
    public bool Enabled { get; set; }

    /// <summary>Gets or sets the local IdleMon agent. Must be a loopback address.</summary>
    public string AgentUrl { get; set; } = "http://127.0.0.1:47100";

    /// <summary>Gets or sets the NAME of the environment variable that holds the per-tool key. The key itself is never in configuration.</summary>
    public string ApiKeyEnv { get; set; } = "IDLEMON_APIKEY";

    /// <summary>Gets or sets <c>source.instance</c>.</summary>
    public string Instance { get; set; } = "default";

    /// <summary>Gets or sets the heartbeat interval in seconds.</summary>
    public int HeartbeatIntervalSeconds { get; set; } = 60;

    /// <summary>Gets or sets the metrics interval in seconds (also the heartbeat tag dataIntervalSeconds).</summary>
    public int MetricsIntervalSeconds { get; set; } = 300;

    /// <summary>Gets or sets the spool folder. Empty means <c>idlemon-spool</c> in the data directory.</summary>
    public string SpoolDirectory { get; set; } = "";

    /// <summary>Gets or sets the spool cap in megabytes.</summary>
    public int SpoolMaxMegabytes { get; set; } = 50;

    /// <summary>Gets or sets a file; when set, batches are written there as JSON Lines instead of being sent.</summary>
    public string DryRunFile { get; set; } = "";

    /// <summary>Gets or sets whether bucket names are sent. When false, A and B are sent instead.</summary>
    public bool IncludeBucketNames { get; set; } = true;

    /// <summary>Gets or sets whether the top-disparities snapshot (which contains object keys) is sent.</summary>
    public bool IncludeObjectKeys { get; set; }

    /// <summary>Gets or sets the overdue alert factor: fires after this many cadences (plus 15 minutes) without a completed run.</summary>
    public double OverdueMultiplier { get; set; } = 2;

    /// <summary>Gets or sets how many cadences a completed run's counts keep being re-sent (at least 10 minutes).</summary>
    public double StaleResultMultiplier { get; set; } = 3;

    /// <summary>Gets or sets the minutes without scan progress before the scan-stuck alert fires.</summary>
    public int StuckScanMinutes { get; set; } = 15;

    /// <summary>Gets the resolved dry-run file, or null when not in dry-run mode.</summary>
    public string? ResolvedDryRunFile => string.IsNullOrWhiteSpace(DryRunFile) ? null : Path.GetFullPath(DryRunFile.Trim().Trim('"'));

    [GeneratedRegex("^[A-Za-z0-9._-]{1,64}$")]
    private static partial Regex NamePattern();

    /// <summary>Gets whether a host name is a loopback name or address.</summary>
    public static bool IsLoopbackHost(string host)
    {
        host = host.Trim('[', ']');
        return host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || (IPAddress.TryParse(host, out var ip) && IPAddress.IsLoopback(ip));
    }

    /// <summary>Returns a description of the first problem with the settings, or null when they are usable.</summary>
    public string? Validate()
    {
        if (!Uri.TryCreate(AgentUrl, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
            return "IdleMon:AgentUrl is not an absolute http(s) address.";
        if (!IsLoopbackHost(uri.Host))
            return "IdleMon:AgentUrl must be a loopback address (localhost, 127.0.0.1 or [::1]); the reporter only talks to this machine.";
        if (string.IsNullOrWhiteSpace(Instance) || !NamePattern().IsMatch(Instance))
            return "IdleMon:Instance must be 1-64 characters from A-Z a-z 0-9 . _ -";
        if (HeartbeatIntervalSeconds is < 10 or > 86400)
            return "IdleMon:HeartbeatIntervalSeconds must be between 10 and 86400.";
        if (MetricsIntervalSeconds is < 5 or > 604800)
            return "IdleMon:MetricsIntervalSeconds must be between 5 and 604800.";
        if (SpoolMaxMegabytes < 1)
            return "IdleMon:SpoolMaxMegabytes must be at least 1.";
        if (OverdueMultiplier <= 0 || StaleResultMultiplier <= 0 || StuckScanMinutes < 1)
            return "IdleMon:OverdueMultiplier, StaleResultMultiplier and StuckScanMinutes must be positive.";
        return null;
    }
}
