using Amazon.S3;
using Amazon.S3.Model;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

public class S3PagingEnumerator : IS3ObjectStream
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _prefix;
    private readonly string? _startAfter;
    private readonly Action<List<S3ObjectSummary>, string?>? _onPageLoaded;
    
    private string? _continuationToken;
    private bool _isTruncated = true;
    private bool _isFirstRequest = true;
    private readonly Queue<S3ObjectSummary> _currentBuffer = new();
    private S3ObjectSummary? _current;
    private long _totalObjectsRead;
    private long _totalBytesRead;

    public S3PagingEnumerator(
        IAmazonS3 s3Client, 
        string bucketName, 
        string prefix,
        string? startAfter = null,
        Action<List<S3ObjectSummary>, string?>? onPageLoaded = null)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _prefix = prefix ?? string.Empty;
        _startAfter = string.IsNullOrWhiteSpace(startAfter) ? null : startAfter.Trim();
        _onPageLoaded = onPageLoaded;
    }

    public S3ObjectSummary? Current => _current;
    public long TotalObjectsRead => _totalObjectsRead;
    public long TotalBytesRead => _totalBytesRead;
    public bool HasMore => _currentBuffer.Count > 0 || _isTruncated;

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        if (_currentBuffer.Count > 0)
        {
            _current = _currentBuffer.Dequeue();
            return true;
        }

        if (!_isTruncated)
        {
            _current = null;
            return false;
        }

        // Fetch next page
        var request = new ListObjectsV2Request
        {
            BucketName = _bucketName,
            Prefix = string.IsNullOrWhiteSpace(_prefix) ? null : _prefix,
            ContinuationToken = _continuationToken,
            StartAfter = _isFirstRequest ? _startAfter : null,
            MaxKeys = 1000
        };
        _isFirstRequest = false;

        var response = await _s3Client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
        _continuationToken = response.NextContinuationToken;
        _isTruncated = response.IsTruncated ?? false;

        var pageItems = new List<S3ObjectSummary>(response.S3Objects.Count);
        foreach (var obj in response.S3Objects)
        {
            var summary = new S3ObjectSummary
            {
                Key = obj.Key,
                Size = obj.Size ?? 0,
                ETag = (obj.ETag ?? string.Empty).Trim('\"'),
                LastModified = obj.LastModified ?? DateTime.MinValue,
                StorageClass = obj.StorageClass?.Value ?? "STANDARD"
            };
            pageItems.Add(summary);
            _currentBuffer.Enqueue(summary);
            _totalObjectsRead++;
            _totalBytesRead += summary.Size;
        }

        _onPageLoaded?.Invoke(pageItems, _continuationToken);

        if (_currentBuffer.Count > 0)
        {
            _current = _currentBuffer.Dequeue();
            return true;
        }

        // If page had 0 objects but isTruncated was true, recurse
        if (_isTruncated)
        {
            return await MoveNextAsync(cancellationToken).ConfigureAwait(false);
        }

        _current = null;
        return false;
    }

    public ValueTask DisposeAsync()
    {
        _currentBuffer.Clear();
        return ValueTask.CompletedTask;
    }
}
