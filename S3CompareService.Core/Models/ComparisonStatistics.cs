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
    public long ContentCheckErrorCount { get; set; }

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

    // Key-range partitioning (see README "Parallel scanning"). A non-partitioned scan has one range.
    public int PartitionsTotal { get; set; }
    public int PartitionsCompleted { get; set; }
    public int PartitionsActive { get; set; }
}

/// <summary>
/// One key range of a partitioned scan, over keys RELATIVE to each bucket's own prefix: (<see cref="Lower"/> exclusive,
/// <see cref="Upper"/> inclusive]. A null <see cref="Lower"/> is the start of the keyspace, a null <see cref="Upper"/> its end.
/// The ranges of a plan tile the keyspace exactly, in order. Also the persisted per-range progress of a checkpoint.
/// </summary>
public sealed class PartitionRange
{
    public string? Lower { get; set; }
    public string? Upper { get; set; }

    /// <summary>The last relative key of this range whose differences are durably stored, or null when none was processed yet.</summary>
    public string? LastCompletedKey { get; set; }

    /// <summary>True when every key of the range has been processed.</summary>
    public bool Completed { get; set; }

    /// <summary>The relative key a listing of this range starts after: <see cref="LastCompletedKey"/>, else <see cref="Lower"/> (null = from the start).</summary>
    [System.Text.Json.Serialization.JsonIgnore]
    public string? ResumeAfter => LastCompletedKey ?? Lower;

    [System.Text.Json.Serialization.JsonIgnore]
    public bool HasProgress => Completed || LastCompletedKey != null;

    /// <summary>True while a worker compares this range (live only; not stored in checkpoints).</summary>
    [System.Text.Json.Serialization.JsonIgnore]
    public bool Active { get; set; }

    /// <summary>Keys compared in this range by the current process (live only; not stored in checkpoints).</summary>
    [System.Text.Json.Serialization.JsonIgnore]
    public long ObjectsCompared { get; set; }

    public PartitionRange Clone() => new() { Lower = Lower, Upper = Upper, LastCompletedKey = LastCompletedKey, Completed = Completed, Active = Active, ObjectsCompared = ObjectsCompared };

    public override string ToString() => $"({Lower ?? "-inf"}, {Upper ?? "+inf"}]{(Completed ? " done" : LastCompletedKey != null ? " @" + LastCompletedKey : string.Empty)}";
}

public class ScanCheckpoint
{
    /// <summary>1 = single key (<see cref="LastEvaluatedKey"/>); 2 = partition plan in <see cref="Partitions"/>.</summary>
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

    /// <summary>
    /// True once at least one key has been fully processed. Needed because the relative key can legitimately be empty
    /// (an object whose key equals the prefix). Legacy checkpoints without this flag are treated as "has key" when
    /// LastEvaluatedKey is non-empty (see <see cref="HasKey"/>).
    /// </summary>
    public bool HasLastEvaluatedKey { get; set; }

    [System.Text.Json.Serialization.JsonIgnore]
    public bool HasKey => HasLastEvaluatedKey || !string.IsNullOrEmpty(LastEvaluatedKey);

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
    public long ContentCheckErrorCount { get; set; }
    public long BucketAObjectsRead { get; set; }
    public long BucketBObjectsRead { get; set; }
    public long BucketABytesRead { get; set; }
    public long BucketBBytesRead { get; set; }
    public double ElapsedSeconds { get; set; }

    public bool DetectSequenceGaps { get; set; } = true;
    public int MaxSequenceGapThreshold { get; set; } = 25;

    /// <summary>
    /// The partition plan with per-range progress (version 2). Stored in its own column (<c>Checkpoints.PartitionPlanJson</c>),
    /// not in the checkpoint JSON. Null for a version 1 checkpoint, which resumes as one range after <see cref="LastEvaluatedKey"/>.
    /// For a plan, <see cref="LastEvaluatedKey"/> is the frontier: every key up to it is done (other ranges may be further).
    /// </summary>
    [System.Text.Json.Serialization.JsonIgnore]
    public List<PartitionRange>? Partitions { get; set; }

    /// <summary>True when the checkpoint records any progress to continue from.</summary>
    [System.Text.Json.Serialization.JsonIgnore]
    public bool CanResume => HasKey || (Partitions?.Any(p => p.HasProgress) ?? false);
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
