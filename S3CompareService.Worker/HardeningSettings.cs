using System.Diagnostics;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Storage;

namespace S3CompareService.Worker;

/// <summary>Reads the long-run hardening settings (<c>S3Compare:Retention:*</c>, <c>S3Compare:MaxRecordedDifferencesPerRun</c>,
/// <c>S3Compare:MaxListRequestsPerSecond</c>, <c>S3Compare:ProcessPriority</c>). Invalid values fall back to the defaults.</summary>
public static class HardeningSettings
{
    /// <summary>The configuration key of the process priority.</summary>
    public const string ProcessPriorityKey = "S3Compare:ProcessPriority";

    /// <summary>Reads the retention settings.</summary>
    public static RetentionOptions Retention(IConfiguration configuration)
    {
        var options = new RetentionOptions();
        configuration.GetSection("S3Compare:Retention").Bind(options);
        return options.Normalized();
    }

    /// <summary>Reads the scan limits.</summary>
    public static ScanOptions Scan(IConfiguration configuration)
    {
        var options = new ScanOptions();
        if (long.TryParse(configuration["S3Compare:MaxRecordedDifferencesPerRun"], out var cap) && cap >= 0)
            options.MaxRecordedDifferencesPerRun = cap;
        if (double.TryParse(configuration["S3Compare:MaxListRequestsPerSecond"], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var rate)
            && rate >= 0 && !double.IsNaN(rate) && !double.IsInfinity(rate))
            options.MaxListRequestsPerSecond = rate;
        return options;
    }

    /// <summary>Parses a priority name. Empty or unknown means BelowNormal; RealTime is refused (it can starve the host).</summary>
    public static ProcessPriorityClass ParsePriority(string? value)
    {
        if (!string.IsNullOrWhiteSpace(value)
            && Enum.TryParse<ProcessPriorityClass>(value.Trim(), ignoreCase: true, out var parsed)
            && parsed != ProcessPriorityClass.RealTime)
            return parsed;
        return ProcessPriorityClass.BelowNormal;
    }

    /// <summary>Sets the priority class of the current process. Never throws; returns the class that is now in effect.</summary>
    public static ProcessPriorityClass? ApplyPriority(string? value, Action<string>? warn = null)
    {
        var wanted = ParsePriority(value);
        try
        {
            using var process = Process.GetCurrentProcess();
            if (process.PriorityClass != wanted) process.PriorityClass = wanted;
            return process.PriorityClass;
        }
        catch (Exception ex) when (ex is InvalidOperationException or System.ComponentModel.Win32Exception or PlatformNotSupportedException)
        {
            warn?.Invoke($"Could not set the process priority to {wanted} ({ex.GetType().Name}).");
            return null;
        }
    }
}
