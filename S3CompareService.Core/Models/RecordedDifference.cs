namespace S3CompareService.Core.Models;

public class RecordedDifference
{
    public long Id { get; set; }
    public string ScanRunId { get; set; } = string.Empty;
    public string Key { get; set; } = string.Empty;
    public DifferenceType Type { get; set; }
    public string Details { get; set; } = string.Empty;

    public long? BucketASize { get; set; }
    public long? BucketBSize { get; set; }
    public string? BucketAETag { get; set; }
    public string? BucketBETag { get; set; }
    public DateTime? BucketALastModified { get; set; }
    public DateTime? BucketBLastModified { get; set; }
    public string? BucketAStorageClass { get; set; }
    public string? BucketBStorageClass { get; set; }

    public DateTime DetectedAtUtc { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// True if either object's LastModified falls within the replication grace period window.
    /// This prevents recent replications from causing false alarm disparities while preserving
    /// the full raw record in the database.
    /// </summary>
    public bool IsWithinGracePeriod { get; set; }
    public DateTime? GracePeriodCutoffUtc { get; set; }
    public string ScanWindowTag => IsWithinGracePeriod ? "IN_FLIGHT_REPLICATION" : "CONFIRMED_DISPARITY";

    public string TypeDisplay => Type switch
    {
        DifferenceType.MissingInBucketB => "Missing in Bucket B",
        DifferenceType.MissingInBucketA => "Missing in Bucket A",
        DifferenceType.SizeMismatch => "Size Mismatch",
        DifferenceType.ETagMismatch => "ETag Mismatch",
        DifferenceType.ContentMismatch => "Content Mismatch",
        DifferenceType.PossibleMissingOnBothSides => "Possible Gap (Both Sides)",
        _ => Type.ToString()
    };

    public string BucketASizeDisplay => BucketASize.HasValue ? S3ObjectSummary.FormatSize(BucketASize.Value) : "-";
    public string BucketBSizeDisplay => BucketBSize.HasValue ? S3ObjectSummary.FormatSize(BucketBSize.Value) : "-";

    public string MetadataSummary
    {
        get
        {
            if (Type == DifferenceType.MissingInBucketB && BucketASize.HasValue)
            {
                return $"A: {BucketASizeDisplay} | ETag: {BucketAETag ?? "-"} | Class: {BucketAStorageClass ?? "STANDARD"} | Mod: {BucketALastModified:yyyy-MM-dd HH:mm:ss}";
            }
            if (Type == DifferenceType.MissingInBucketA && BucketBSize.HasValue)
            {
                return $"B: {BucketBSizeDisplay} | ETag: {BucketBETag ?? "-"} | Class: {BucketBStorageClass ?? "STANDARD"} | Mod: {BucketBLastModified:yyyy-MM-dd HH:mm:ss}";
            }
            if (Type == DifferenceType.PossibleMissingOnBothSides)
            {
                return "Synthetic sequence gap (missing in both buckets)";
            }
            return $"A: {BucketASizeDisplay} vs B: {BucketBSizeDisplay}";
        }
    }
}
