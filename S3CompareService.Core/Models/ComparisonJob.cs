namespace S3CompareService.Core.Models;

public class ComparisonJob
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string Name { get; set; } = "Default Bucket Compare";
    public string Description { get; set; } = string.Empty;

    public S3BucketConfig BucketA { get; set; } = new();
    public S3BucketConfig BucketB { get; set; } = new();

    public ComparisonMode Mode { get; set; } = ComparisonMode.ListingOnly;

    public JobScheduleType ScheduleType { get; set; } = JobScheduleType.IntervalMinutes;
    public int IntervalMinutes { get; set; } = 60;
    public string CronExpression { get; set; } = string.Empty;

    public bool IsEnabled { get; set; } = true;

    /// <summary>
    /// Files with LastModified within this many minutes of scan start are flagged
    /// as in-flight replication to prevent false alarm disparities.
    /// 0 disables grace period clipping/tagging.
    /// </summary>
    public int ReplicationGracePeriodMinutes { get; set; } = 15;

    public bool DetectSequenceGaps { get; set; } = true;
    public int MaxSequenceGapThreshold { get; set; } = 25;

    public JobAlertConfig AlertConfig { get; set; } = new();

    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime? LastRunTimeUtc { get; set; }
    public DateTime? NextRunTimeUtc { get; set; }
    public RunStatus? LastRunStatus { get; set; }
}
