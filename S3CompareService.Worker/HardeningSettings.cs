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
    /// <c>PartitionOversubscription</c>, <c>MaxDiscoveryDepth</c>). A missing value keeps its default. A value above its
    /// limit is clamped to the limit; an unreadable or too small value keeps the default. Both are reported through
    /// <paramref name="warn"/>.
    /// </summary>
    public static ScanOptions Scan(IConfiguration configuration, Action<string>? warn = null)
    {
        var options = new ScanOptions();
        if (long.TryParse(configuration["S3Compare:MaxRecordedDifferencesPerRun"], out var cap) && cap >= 0)
            options.MaxRecordedDifferencesPerRun = cap;
        options.MaxListRequestsPerSecond = ReadDouble(configuration, "MaxListRequestsPerSecond", options.MaxListRequestsPerSecond, ScanOptions.MaxListRequestsPerSecondLimit, warn);
        options.MaxObjectsPerSecond = ReadDouble(configuration, "MaxObjectsPerSecond", options.MaxObjectsPerSecond, ScanOptions.MaxObjectsPerSecondLimit, warn);
        options.MaxParallelPartitions = ReadInt(configuration, "MaxParallelPartitions", options.MaxParallelPartitions, 1, ScanOptions.MaxParallelPartitionsLimit, warn);
        options.PrefetchPages = ReadInt(configuration, "PrefetchPages", options.PrefetchPages, 1, ScanOptions.PrefetchPagesLimit, warn);
        options.MaxConcurrentContentChecks = ReadInt(configuration, "MaxConcurrentContentChecks", options.MaxConcurrentContentChecks, 1, ScanOptions.MaxConcurrentContentChecksLimit, warn);
        options.MaxDiscoveryRequests = ReadInt(configuration, "MaxDiscoveryRequests", options.MaxDiscoveryRequests, 0, ScanOptions.MaxDiscoveryRequestsLimit, warn);
        options.PartitionOversubscription = ReadInt(configuration, "PartitionOversubscription", options.PartitionOversubscription, 1, ScanOptions.PartitionOversubscriptionLimit, warn);
        options.MaxDiscoveryDepth = ReadInt(configuration, "MaxDiscoveryDepth", options.MaxDiscoveryDepth, 0, ScanOptions.MaxDiscoveryDepthLimit, warn);
        if (options.ApplyMemoryCap())
        {
            warn?.Invoke($"S3Compare:PrefetchPages lowered to {options.PrefetchPages}: MaxParallelPartitions x (PrefetchPages + 1) may not exceed {ScanOptions.MaxBufferedPagesPerBucket} list pages per bucket.");
        }
        return options;
    }

    private static double ReadDouble(IConfiguration configuration, string name, double fallback, double max, Action<string>? warn)
    {
        string? text = configuration["S3Compare:" + name];
        if (string.IsNullOrWhiteSpace(text)) return fallback;
        if (!double.TryParse(text, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var value)
            || double.IsNaN(value) || value < 0)
        {
            warn?.Invoke($"S3Compare:{name} = '{text}' is not a number of at least 0; using the default {fallback}.");
            return fallback;
        }
        if (value > max)
        {
            warn?.Invoke($"S3Compare:{name} = {text} is above the limit; using {max}.");
            return max;
        }
        return value;
    }

    private static int ReadInt(IConfiguration configuration, string name, int fallback, int min, int max, Action<string>? warn)
    {
        string? text = configuration["S3Compare:" + name];
        if (string.IsNullOrWhiteSpace(text)) return fallback;
        if (!long.TryParse(text, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var value) || value < min)
        {
            warn?.Invoke($"S3Compare:{name} = '{text}' is not a whole number of at least {min}; using the default {fallback}.");
            return fallback;
        }
        if (value > max)
        {
            warn?.Invoke($"S3Compare:{name} = {text} is above the limit; using {max}.");
            return max;
        }
        return (int)value;
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
