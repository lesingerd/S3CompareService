namespace S3CompareService.Core.Models;

public enum ComparisonMode
{
    ListingOnly,
    FullData
}

public enum DifferenceType
{
    MissingInBucketB,
    MissingInBucketA,
    SizeMismatch,
    ETagMismatch,
    ContentMismatch,
    PossibleMissingOnBothSides
}

public enum JobScheduleType
{
    IntervalMinutes,
    CronExpression,
    Manual
}

public enum RunStatus
{
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Interrupted,
    Cancelled
}
