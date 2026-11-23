using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

/// <summary>One ListObjectsV2 response: objects in UTF-8 key order, common prefixes (with a delimiter) and paging state.</summary>
public sealed class S3ListPage
{
    public List<S3ObjectSummary> Objects { get; init; } = new();
    public List<string> CommonPrefixes { get; init; } = new();
    public bool IsTruncated { get; init; }
    public string? NextContinuationToken { get; init; }
}

/// <summary>
/// The one S3 call the scanner and the partition planner need: a single ListObjectsV2 page. The real implementation is
/// <see cref="S3Lister"/>; tests use a fake that honours the same parameters.
/// </summary>
public interface IS3Lister
{
    /// <param name="bucket">Bucket name.</param>
    /// <param name="prefix">Only keys starting with this (null or empty = all).</param>
    /// <param name="startAfter">Only keys after this full key (first request only; null = from the start).</param>
    /// <param name="continuationToken">The previous page's <see cref="S3ListPage.NextContinuationToken"/>, or null.</param>
    /// <param name="delimiter">Group keys by this delimiter into <see cref="S3ListPage.CommonPrefixes"/> (null = flat).</param>
    /// <param name="maxKeys">Most entries (objects plus common prefixes) per page; S3 caps it at 1000.</param>
    Task<S3ListPage> ListPageAsync(
        string bucket,
        string? prefix,
        string? startAfter,
        string? continuationToken,
        string? delimiter,
        int maxKeys,
        CancellationToken cancellationToken);
}

/// <summary>
/// Full Data mode: opens an object's content for reading. <see cref="S3Lister"/> implements it with GetObject; a lister that
/// does not implement it makes Full Data scans fall back to the listing comparison (ETag mismatch) as without S3 clients.
/// </summary>
public interface IS3ObjectReader
{
    /// <summary>The object's content. The caller disposes the stream (which releases the connection).</summary>
    Task<Stream> OpenReadAsync(string bucket, string key, CancellationToken cancellationToken);
}

/// <summary><see cref="IS3Lister"/> and <see cref="IS3ObjectReader"/> over an <see cref="IAmazonS3"/> client. Does not own (dispose) the client.</summary>
public sealed class S3Lister : IS3Lister, IS3ObjectReader
{
    private readonly IAmazonS3 _client;
    private readonly int _maxAttempts;

    /// <param name="client">The SDK client. Does not own it.</param>
    /// <param name="maxAttempts">Tries per call (1 = no retry). The scan uses clients without SDK retries and retries here, so each attempt shows in the diagnostics.</param>
    public S3Lister(IAmazonS3 client, int maxAttempts = 1)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _maxAttempts = Math.Max(1, maxAttempts);
    }

    /// <summary>Retries a call on transient failures (5xx, throttling, timeouts, network errors) with a short back-off, reporting the attempt number.</summary>
    private async Task<T> WithRetryAsync<T>(Func<Task<T>> call, CancellationToken ct)
    {
        for (int attempt = 1; ; attempt++)
        {
            var op = InFlightTracker.CurrentRequest.Value;
            if (op != null) op.Attempt = attempt;
            long started = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                return await call().ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < _maxAttempts && !ct.IsCancellationRequested && IsTransient(ex))
            {
                if (op != null)
                {
                    op.RetryReason = FailureReason(ex, System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalSeconds);
                    op.Retries = attempt;
                }
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(5000, 400 * Math.Pow(2, attempt - 1))), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>A short reason for the diagnostics: "timed out after 30 s", "SlowDown (503)" or "TypeName: message".</summary>
    internal static string FailureReason(Exception ex, double seconds)
    {
        if (ex is TaskCanceledException or TimeoutException || ex.InnerException is TaskCanceledException or TimeoutException)
            return $"timed out after {seconds:0} s";
        if (ex is AmazonServiceException s && !string.IsNullOrEmpty(s.ErrorCode)) return $"{s.ErrorCode} ({(int)s.StatusCode})";
        string msg = ex.Message ?? string.Empty;
        return $"{ex.GetType().Name}: {(msg.Length <= 120 ? msg : msg.Substring(0, 117) + "...")}";
    }

    /// <summary>True for failures worth another try: server errors, throttling, timeouts and network errors (not 4xx such as AccessDenied).</summary>
    public static bool IsTransient(Exception ex) => ex switch
    {
        AmazonServiceException s => (int)s.StatusCode >= 500 || (int)s.StatusCode is 408 or 429 || (s.ErrorCode?.Contains("SlowDown", StringComparison.OrdinalIgnoreCase) ?? false) || (s.ErrorCode?.Contains("Throttl", StringComparison.OrdinalIgnoreCase) ?? false) || (s.ErrorCode?.Contains("RequestTimeout", StringComparison.OrdinalIgnoreCase) ?? false),
        Amazon.Runtime.AmazonClientException => true,
        HttpRequestException or IOException or TimeoutException or TaskCanceledException => true,
        _ => false
    };

    public async Task<S3ListPage> ListPageAsync(
        string bucket,
        string? prefix,
        string? startAfter,
        string? continuationToken,
        string? delimiter,
        int maxKeys,
        CancellationToken cancellationToken)
    {
        var request = new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = string.IsNullOrEmpty(prefix) ? null : prefix,
            StartAfter = string.IsNullOrEmpty(startAfter) ? null : startAfter,
            ContinuationToken = string.IsNullOrEmpty(continuationToken) ? null : continuationToken,
            Delimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter,
            MaxKeys = Math.Clamp(maxKeys, 1, 1000)
        };

        var response = await WithRetryAsync(() => _client.ListObjectsV2Async(request, cancellationToken), cancellationToken).ConfigureAwait(false);
        var objects = response.S3Objects ?? new List<S3Object>();
        var items = new List<S3ObjectSummary>(objects.Count);
        foreach (var obj in objects) items.Add(ToSummary(obj));

        return new S3ListPage
        {
            Objects = items,
            CommonPrefixes = response.CommonPrefixes?.ToList() ?? new List<string>(),
            IsTruncated = response.IsTruncated ?? false,
            NextContinuationToken = response.NextContinuationToken
        };
    }

    public async Task<Stream> OpenReadAsync(string bucket, string key, CancellationToken cancellationToken)
    {
        // Disposing the response stream releases the HTTP connection; the response object holds nothing else.
        var response = await WithRetryAsync(() => _client.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key }, cancellationToken), cancellationToken).ConfigureAwait(false);
        return response.ResponseStream;
    }

    /// <summary>Maps an SDK object to the engine's summary (ETag without quotes, UTC time, STANDARD when no class is given).</summary>
    public static S3ObjectSummary ToSummary(S3Object obj) => new()
    {
        Key = obj.Key,
        Size = obj.Size ?? 0,
        ETag = (obj.ETag ?? string.Empty).Trim('\"'),
        LastModified = obj.LastModified?.ToUniversalTime() ?? DateTime.MinValue,
        StorageClass = obj.StorageClass?.Value ?? "STANDARD"
    };
}
