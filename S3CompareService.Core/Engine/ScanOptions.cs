namespace S3CompareService.Core.Engine;

/// <summary>
/// Limits that keep a scan from harming the host, the database or the S3 endpoints. Bound from
/// <c>S3Compare:MaxRecordedDifferencesPerRun</c> and <c>S3Compare:MaxListRequestsPerSecond</c>; the defaults here are the
/// production defaults.
/// </summary>
public sealed class ScanOptions
{
    /// <summary>Default for <see cref="MaxRecordedDifferencesPerRun"/>.</summary>
    public const int DefaultMaxRecordedDifferencesPerRun = 100_000;

    /// <summary>Default for <see cref="MaxListRequestsPerSecond"/>.</summary>
    public const double DefaultMaxListRequestsPerSecond = 20;

    /// <summary>
    /// The most difference rows stored per run. Beyond it the counters keep counting every difference but no more rows are
    /// inserted and the run is flagged <c>DifferencesTruncated</c>. 0 or less means unlimited.
    /// </summary>
    public long MaxRecordedDifferencesPerRun { get; set; } = DefaultMaxRecordedDifferencesPerRun;

    /// <summary>
    /// The most S3 requests per second (list pages, and object reads in FullData mode) each scan sends to each bucket.
    /// 0 or less means unlimited.
    /// </summary>
    public double MaxListRequestsPerSecond { get; set; } = DefaultMaxListRequestsPerSecond;
}
