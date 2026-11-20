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

    /// <summary>Reads the notification settings (<c>S3Compare:Notifications:DeliveryTimeoutSeconds</c>). Invalid values fall back to the default.</summary>
    public static S3CompareService.Core.Notifications.NotificationOptions Notifications(IConfiguration configuration)
    {
        var options = new S3CompareService.Core.Notifications.NotificationOptions();
        if (int.TryParse(configuration["S3Compare:Notifications:DeliveryTimeoutSeconds"], out var seconds) && seconds >= 1)
            options.DeliveryTimeoutSeconds = seconds;
        return options;
    }

    /// <summary>
    /// Reads the scan limits and the parallel-scan throughput defaults (<c>S3Compare:MaxParallelPartitions</c>,
    /// <c>PrefetchPages</c>, <c>MaxObjectsPerSecond</c>, <c>MaxConcurrentContentChecks</c>, <c>MaxDiscoveryRequests</c>,
    /// <c>PartitionOversubscription</c>, <c>MaxDiscoveryDepth</c>). A missing or out-of-range value keeps its default.
    /// </summary>
    public static ScanOptions Scan(IConfiguration configuration)
    {
        var options = new ScanOptions();
        if (long.TryParse(configuration["S3Compare:MaxRecordedDifferencesPerRun"], out var cap) && cap >= 0)
            options.MaxRecordedDifferencesPerRun = cap;
        if (TryDouble(configuration["S3Compare:MaxListRequestsPerSecond"], ScanOptions.MaxListRequestsPerSecondLimit, out var rate))
            options.MaxListRequestsPerSecond = rate;
        if (TryDouble(configuration["S3Compare:MaxObjectsPerSecond"], ScanOptions.MaxObjectsPerSecondLimit, out var objects))
            options.MaxObjectsPerSecond = objects;
        if (TryInt(configuration["S3Compare:MaxParallelPartitions"], 1, ScanOptions.MaxParallelPartitionsLimit, out var partitions))
            options.MaxParallelPartitions = partitions;
        if (TryInt(configuration["S3Compare:PrefetchPages"], 1, ScanOptions.PrefetchPagesLimit, out var prefetch))
            options.PrefetchPages = prefetch;
        if (TryInt(configuration["S3Compare:MaxConcurrentContentChecks"], 1, ScanOptions.MaxConcurrentContentChecksLimit, out var checks))
            options.MaxConcurrentContentChecks = checks;
        if (TryInt(configuration["S3Compare:MaxDiscoveryRequests"], 0, ScanOptions.MaxDiscoveryRequestsLimit, out var discovery))
            options.MaxDiscoveryRequests = discovery;
        if (TryInt(configuration["S3Compare:PartitionOversubscription"], 1, ScanOptions.PartitionOversubscriptionLimit, out var oversub))
            options.PartitionOversubscription = oversub;
        if (TryInt(configuration["S3Compare:MaxDiscoveryDepth"], 0, ScanOptions.MaxDiscoveryDepthLimit, out var depth))
            options.MaxDiscoveryDepth = depth;
        return options;
    }

    private static bool TryDouble(string? text, double max, out double value) =>
        double.TryParse(text, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out value)
        && value >= 0 && value <= max && !double.IsNaN(value) && !double.IsInfinity(value);

    private static bool TryInt(string? text, int min, int max, out int value) =>
        int.TryParse(text, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out value)
        && value >= min && value <= max;

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
