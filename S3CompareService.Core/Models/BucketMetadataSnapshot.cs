namespace S3CompareService.Core.Models;

public class BucketMetadataSnapshot
{
    public long Id { get; set; }
    public string ScanRunId { get; set; } = string.Empty;
    public string BucketSide { get; set; } = "A"; // "A" or "B"
    public string BucketName { get; set; } = string.Empty;
    public string EndpointUrl { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public string Prefix { get; set; } = string.Empty;
    public DateTime SnapshotTimeUtc { get; set; } = DateTime.UtcNow;

    public long TotalObjects { get; set; }
    public long TotalBytes { get; set; }
    public string FormattedTotalSize => S3ObjectSummary.FormatSize(TotalBytes);

    public string? VersioningStatus { get; set; }
    public string StorageClassesJson { get; set; } = "{}";
    public DateTime? OldestLastModifiedUtc { get; set; }
    public DateTime? NewestLastModifiedUtc { get; set; }
}
