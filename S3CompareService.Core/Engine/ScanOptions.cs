using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

/// <summary>
/// Limits that keep a scan from harming the host, the database or the S3 endpoints, and the throughput settings of the
/// parallel scanner. Bound from <c>S3Compare:*</c> (see <c>HardeningSettings.Scan</c> in the Worker); the defaults here are
/// the production defaults. A job can override the throughput settings (<see cref="ComparisonJob.Performance"/>); use
/// <see cref="ForJob"/> to get the effective options of one scan.
/// </summary>
public sealed class ScanOptions
{
    /// <summary>Default for <see cref="MaxRecordedDifferencesPerRun"/>.</summary>
    public const int DefaultMaxRecordedDifferencesPerRun = 100_000;

    /// <summary>Default for <see cref="MaxListRequestsPerSecond"/>.</summary>
    public const double DefaultMaxListRequestsPerSecond = 20;

    public const int DefaultMaxParallelPartitions = 8;
    public const int DefaultPrefetchPages = 2;
    public const double DefaultMaxObjectsPerSecond = 0;
    public const int DefaultMaxConcurrentContentChecks = 8;
    public const int DefaultMaxDiscoveryRequests = 500;
    public const int DefaultPartitionOversubscription = 4;
    public const int DefaultMaxDiscoveryDepth = 3;

    // Accepted ranges (API validation; configuration values outside them fall back to the default).
    public const int MaxParallelPartitionsLimit = 256;
    public const int PrefetchPagesLimit = 64;
    public const double MaxListRequestsPerSecondLimit = 100_000;
    public const double MaxObjectsPerSecondLimit = 100_000_000;
    public const int MaxConcurrentContentChecksLimit = 256;
    public const int MaxDiscoveryRequestsLimit = 100_000;
    public const int PartitionOversubscriptionLimit = 64;
    public const int MaxDiscoveryDepthLimit = 10;

    /// <summary>
    /// Memory bound: list pages one bucket side may hold at once, <c>MaxParallelPartitions x (PrefetchPages + 1)</c> (each
    /// listing holds its prefetched pages plus the page being compared; a page is up to 1,000 object summaries). With both
    /// buckets that is at most about 1 million summaries in flight.
    /// </summary>
    public const int MaxBufferedPagesPerBucket = 512;

    /// <summary>
    /// The most difference rows stored per run. Beyond it the counters keep counting every difference but no more rows are
    /// inserted and the run is flagged <c>DifferencesTruncated</c>. 0 or less means unlimited.
    /// </summary>
    public long MaxRecordedDifferencesPerRun { get; set; } = DefaultMaxRecordedDifferencesPerRun;

    /// <summary>
    /// The most S3 requests per second (list pages, planner requests, and object reads in FullData mode) each scan sends to
    /// each bucket. Shared by all partitions of the scan. 0 or less means unlimited.
    /// </summary>
    public double MaxListRequestsPerSecond { get; set; } = DefaultMaxListRequestsPerSecond;

    /// <summary>Key ranges compared at the same time. 1 = no partitioning (one listing per bucket, still with prefetch).</summary>
    public int MaxParallelPartitions { get; set; } = DefaultMaxParallelPartitions;

    /// <summary>List pages a bucket listing fetches ahead of the comparison, per partition (bounded buffer).</summary>
    public int PrefetchPages { get; set; } = DefaultPrefetchPages;

    /// <summary>
    /// Compared keys per second across all partitions (a key present in both buckets counts once, like a key present in one).
    /// 0 or less means unlimited.
    /// </summary>
    public double MaxObjectsPerSecond { get; set; } = DefaultMaxObjectsPerSecond;

    /// <summary>Full Data mode: object pairs downloaded and compared at the same time across all partitions.</summary>
    public int MaxConcurrentContentChecks { get; set; } = DefaultMaxConcurrentContentChecks;

    /// <summary>Most S3 requests the partition planner may send per scan (both buckets). 0 = no planning: one range.</summary>
    public int MaxDiscoveryRequests { get; set; } = DefaultMaxDiscoveryRequests;

    /// <summary>Ranges planned per parallel partition (target ranges = partitions x this), so uneven ranges balance out.</summary>
    public int PartitionOversubscription { get; set; } = DefaultPartitionOversubscription;

    /// <summary>How many "/" levels below the prefix the planner explores looking for boundaries.</summary>
    public int MaxDiscoveryDepth { get; set; } = DefaultMaxDiscoveryDepth;

    /// <summary>Keys per list request. S3 caps it at 1000; smaller values are only useful in tests.</summary>
    public int ListPageSize { get; set; } = 1000;

    /// <summary>Number of key ranges the planner aims for.</summary>
    public int TargetPartitions => Math.Max(1, MaxParallelPartitions) * Math.Max(1, PartitionOversubscription);

    /// <summary>A copy of these options.</summary>
    public ScanOptions Clone() => (ScanOptions)MemberwiseClone();

    /// <summary>The effective options for one job: these service-wide values with the job's non-null overrides applied.</summary>
    public ScanOptions ForJob(ComparisonJob? job)
    {
        var o = Clone();
        var p = job?.Performance;
        if (p == null)
        {
            o.ApplyMemoryCap();
            return o;
        }
        if (p.MaxParallelPartitions is int mp) o.MaxParallelPartitions = mp;
        if (p.PrefetchPages is int pf) o.PrefetchPages = pf;
        if (p.MaxListRequestsPerSecond is double lr) o.MaxListRequestsPerSecond = lr;
        if (p.MaxObjectsPerSecond is double ob) o.MaxObjectsPerSecond = ob;
        if (p.MaxConcurrentContentChecks is int cc) o.MaxConcurrentContentChecks = cc;
        if (p.MaxDiscoveryRequests is int dr) o.MaxDiscoveryRequests = dr;
        o.ApplyMemoryCap();
        return o;
    }

    /// <summary>
    /// Lowers <see cref="PrefetchPages"/> (not below 1) so that <c>MaxParallelPartitions x (PrefetchPages + 1)</c> stays within
    /// <see cref="MaxBufferedPagesPerBucket"/>. Returns true when it changed anything.
    /// </summary>
    public bool ApplyMemoryCap()
    {
        int partitions = Math.Max(1, MaxParallelPartitions);
        if ((long)partitions * (Math.Max(1, PrefetchPages) + 1) <= MaxBufferedPagesPerBucket) return false;
        int allowed = Math.Max(1, MaxBufferedPagesPerBucket / partitions - 1);
        if (allowed >= PrefetchPages) return false;
        PrefetchPages = allowed;
        return true;
    }

    /// <summary>Validates per-job overrides. Returns an error message, or null when every set value is in range.</summary>
    public static string? Validate(JobPerformanceSettings? p)
    {
        if (p == null) return null;
        return CheckInt(p.MaxParallelPartitions, nameof(p.MaxParallelPartitions), 1, MaxParallelPartitionsLimit)
            ?? CheckInt(p.PrefetchPages, nameof(p.PrefetchPages), 1, PrefetchPagesLimit)
            ?? CheckDouble(p.MaxListRequestsPerSecond, nameof(p.MaxListRequestsPerSecond), MaxListRequestsPerSecondLimit)
            ?? CheckDouble(p.MaxObjectsPerSecond, nameof(p.MaxObjectsPerSecond), MaxObjectsPerSecondLimit)
            ?? CheckInt(p.MaxConcurrentContentChecks, nameof(p.MaxConcurrentContentChecks), 1, MaxConcurrentContentChecksLimit)
            ?? CheckInt(p.MaxDiscoveryRequests, nameof(p.MaxDiscoveryRequests), 0, MaxDiscoveryRequestsLimit)
            ?? (p.MaxParallelPartitions is int mp && p.PrefetchPages is int pf && (long)mp * (pf + 1) > MaxBufferedPagesPerBucket
                ? $"MaxParallelPartitions x (PrefetchPages + 1) must not exceed {MaxBufferedPagesPerBucket} (list pages held in memory per bucket)."
                : null);
    }

    private static string? CheckInt(int? value, string name, int min, int max) =>
        value is int v && (v < min || v > max) ? $"{name} must be between {min} and {max} (or empty for the service default)." : null;

    private static string? CheckDouble(double? value, string name, double max) =>
        value is double v && (double.IsNaN(v) || double.IsInfinity(v) || v < 0 || v > max)
            ? $"{name} must be between 0 and {max:0} (0 = unlimited, or empty for the service default)."
            : null;
}
