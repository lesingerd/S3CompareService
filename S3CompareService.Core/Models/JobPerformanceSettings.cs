namespace S3CompareService.Core.Models;

/// <summary>
/// Per-job overrides of the scan throughput settings. Every value is optional: null means "use the service-wide default"
/// from <c>S3Compare:*</c> (see <see cref="Engine.ScanOptions"/>). Stored as JSON in the <c>Jobs.Performance_Json</c> column.
/// </summary>
public class JobPerformanceSettings
{
    /// <summary>Key ranges compared at the same time. 1 = no partitioning (one listing per bucket, still with prefetch).</summary>
    public int? MaxParallelPartitions { get; set; }

    /// <summary>List pages each bucket listing may fetch ahead of the comparison (per partition).</summary>
    public int? PrefetchPages { get; set; }

    /// <summary>S3 requests per second per bucket side, shared by all partitions of the scan. 0 = unlimited.</summary>
    public double? MaxListRequestsPerSecond { get; set; }

    /// <summary>Compared keys per second across all partitions. 0 = unlimited.</summary>
    public double? MaxObjectsPerSecond { get; set; }

    /// <summary>Full Data mode: object pairs downloaded and compared at the same time across all partitions.</summary>
    public int? MaxConcurrentContentChecks { get; set; }

    /// <summary>Most S3 requests the partition planner may send (per scan, both buckets together). 0 = no planning (one range).</summary>
    public int? MaxDiscoveryRequests { get; set; }

    /// <summary>Time box of the partition planner in seconds (service default 60). 0 = no limit.</summary>
    public int? MaxPlanningSeconds { get; set; }

    /// <summary>True when no override is set.</summary>
    public bool IsEmpty =>
        MaxParallelPartitions is null && PrefetchPages is null && MaxListRequestsPerSecond is null
        && MaxObjectsPerSecond is null && MaxConcurrentContentChecks is null && MaxDiscoveryRequests is null
        && MaxPlanningSeconds is null;
}
