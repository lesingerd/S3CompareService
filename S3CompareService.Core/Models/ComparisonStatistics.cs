namespace S3CompareService.Core.Models;

public class ComparisonStatistics
{
    public long ObjectsCompared { get; set; }
    public long MatchedCount { get; set; }
    public long TotalDifferences { get; set; }
    public long FilteredDifferencesCount { get; set; }
    public long InFlightDifferencesCount { get; set; }

    public long MissingInBucketBCount { get; set; }
    public long MissingInBucketACount { get; set; }
    public long SizeMismatchCount { get; set; }
    public long ETagMismatchCount { get; set; }
    public long ContentMismatchCount { get; set; }
    public long SequenceGapCount { get; set; }

    public long BucketAObjectsRead { get; set; }
    public long BucketBObjectsRead { get; set; }
    public long BucketABytesRead { get; set; }
    public long BucketBBytesRead { get; set; }

    public TimeSpan ElapsedTime { get; set; }
    public double ItemsPerSecond { get; set; }
    public string CurrentKeyA { get; set; } = "-";
    public string CurrentKeyB { get; set; } = "-";

    // System diagnostics
    public double WorkingSetMB { get; set; }
    public double GCTotalMemoryMB { get; set; }
    public string LastCheckpointKey { get; set; } = "-";
    public DateTime? LastCheckpointTime { get; set; }
}

public class ScanCheckpoint
{
    public int Version { get; set; } = 1;
    public string ScanRunId { get; set; } = string.Empty;
    public string JobId { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime LastUpdatedAtUtc { get; set; } = DateTime.UtcNow;

    public string BucketNameA { get; set; } = string.Empty;
    public string EndpointA { get; set; } = string.Empty;
    public string PrefixA { get; set; } = string.Empty;

    public string BucketNameB { get; set; } = string.Empty;
    public string EndpointB { get; set; } = string.Empty;
    public string PrefixB { get; set; } = string.Empty;

    public ComparisonMode Mode { get; set; }
    public string LastEvaluatedKey { get; set; } = string.Empty;

    public long ObjectsCompared { get; set; }
    public long MatchedCount { get; set; }
    public long TotalDifferences { get; set; }
    public long FilteredDifferencesCount { get; set; }
    public long InFlightDifferencesCount { get; set; }

    public long MissingInBucketBCount { get; set; }
    public long MissingInBucketACount { get; set; }
    public long SizeMismatchCount { get; set; }
    public long ETagMismatchCount { get; set; }
    public long ContentMismatchCount { get; set; }
    public long SequenceGapCount { get; set; }
    public long BucketAObjectsRead { get; set; }
    public long BucketBObjectsRead { get; set; }
    public long BucketABytesRead { get; set; }
    public long BucketBBytesRead { get; set; }
    public double ElapsedSeconds { get; set; }

    public bool DetectSequenceGaps { get; set; } = true;
    public int MaxSequenceGapThreshold { get; set; } = 25;
}

public class ServiceAuditLog
{
    public long Id { get; set; }
    public DateTime TimestampUtc { get; set; } = DateTime.UtcNow;
    public string Level { get; set; } = "INFO"; // INFO, WARN, ERROR, RECOVERY
    public string Category { get; set; } = "Service";
    public string Message { get; set; } = string.Empty;
    public string? Details { get; set; }
    public string? ScanRunId { get; set; }
    public string? JobId { get; set; }
}
