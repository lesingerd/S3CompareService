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
    PossibleMissingOnBothSides,

    /// <summary>Full Data mode: the object content could not be read (access denied, service or network error after one retry). Not a proven difference, but the object is unverified.</summary>
    ContentCheckError
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
