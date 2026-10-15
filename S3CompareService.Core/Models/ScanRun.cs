namespace S3CompareService.Core.Models;

public class ScanRun
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string JobId { get; set; } = string.Empty;
    public string JobName { get; set; } = string.Empty;

    public DateTime StartTimeUtc { get; set; } = DateTime.UtcNow;
    public DateTime? EndTimeUtc { get; set; }
    public RunStatus Status { get; set; } = RunStatus.Pending;
    public ComparisonMode Mode { get; set; } = ComparisonMode.ListingOnly;

    // Replication grace period & scan clipping
    public int ReplicationGracePeriodMinutes { get; set; } = 15;
    public DateTime? GracePeriodCutoffUtc { get; set; }
    public bool IsScanWindowClipped { get; set; }
    public string ScanWindowNotes { get; set; } = string.Empty;

    // Resumption & Checkpoint tracking
    public string LastEvaluatedKey { get; set; } = string.Empty;
    public bool IsResumedFromCheckpoint { get; set; }
    public string? ResumedFromKey { get; set; }

    // Summary counts
    public long ObjectsCompared { get; set; }
    public long MatchedCount { get; set; }
    public long TotalDifferences { get; set; } // Raw total
    public long FilteredDifferencesCount { get; set; } // Confirmed disparities (excluding grace period)
    public long InFlightDifferencesCount { get; set; } // In-flight replication (within grace period)

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

    public double DurationSeconds { get; set; }
    public string? ErrorMessage { get; set; }

    public TimeSpan Duration => TimeSpan.FromSeconds(DurationSeconds);
    public string FormattedDuration => Duration.ToString(@"hh\:mm\:ss");
    public double ItemsPerSecond => DurationSeconds > 0 ? ObjectsCompared / DurationSeconds : 0;
}
