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
    private DateTime? _oldestLastModifiedUtc;
    private DateTime? _newestLastModifiedUtc;
    private readonly Dictionary<string, long> _storageClassCounts = new(StringComparer.OrdinalIgnoreCase);

    public S3PagingEnumerator(
        IAmazonS3 s3Client, 
        string bucketName, 
        string prefix,
        string? startAfter = null,
        Action<List<S3ObjectSummary>, string?>? onPageLoaded = null,
        long initialObjectsRead = 0,
        long initialBytesRead = 0)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _prefix = prefix ?? string.Empty;
        _startAfter = string.IsNullOrEmpty(startAfter) ? null : startAfter;
        _onPageLoaded = onPageLoaded;
        _totalObjectsRead = initialObjectsRead;
        _totalBytesRead = initialBytesRead;
    }

    public S3ObjectSummary? Current => _current;
    public long TotalObjectsRead => _totalObjectsRead;
    public long TotalBytesRead => _totalBytesRead;
    public DateTime? OldestLastModifiedUtc => _oldestLastModifiedUtc;
    public DateTime? NewestLastModifiedUtc => _newestLastModifiedUtc;
    public IReadOnlyDictionary<string, long> StorageClassCounts => _storageClassCounts;
    public bool HasMore => _currentBuffer.Count > 0 || _isTruncated;

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        while (_currentBuffer.Count == 0 && _isTruncated)
        {
            var request = new ListObjectsV2Request
            {
                BucketName = _bucketName,
                Prefix = string.IsNullOrEmpty(_prefix) ? null : _prefix,
                ContinuationToken = _continuationToken,
                StartAfter = _isFirstRequest ? _startAfter : null,
                MaxKeys = 1000
            };
            _isFirstRequest = false;

            var response = await _s3Client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
            _continuationToken = response.NextContinuationToken;
            _isTruncated = response.IsTruncated ?? false;

            if (_isTruncated && string.IsNullOrEmpty(_continuationToken))
            {
                throw new InvalidOperationException(
                    $"S3 listing of bucket '{_bucketName}' reported IsTruncated=true but returned no NextContinuationToken; refusing to loop. The endpoint may not fully support ListObjectsV2 pagination.");
            }

            var objects = response.S3Objects ?? new List<S3Object>();
            var pageItems = new List<S3ObjectSummary>(objects.Count);
            foreach (var obj in objects)
            {
                var summary = new S3ObjectSummary
                {
                    Key = obj.Key,
                    Size = obj.Size ?? 0,
                    ETag = (obj.ETag ?? string.Empty).Trim('\"'),
                    LastModified = obj.LastModified?.ToUniversalTime() ?? DateTime.MinValue,
                    StorageClass = obj.StorageClass?.Value ?? "STANDARD"
                };
                pageItems.Add(summary);
                _currentBuffer.Enqueue(summary);
            }

            _onPageLoaded?.Invoke(pageItems, _continuationToken);
        }

        if (_currentBuffer.Count > 0)
        {
            _current = _currentBuffer.Dequeue();

            // Statistics are counted on consumption (not on page load) so checkpoints never include
            // objects that were fetched but not yet handed to the comparison loop.
            _totalObjectsRead++;
            _totalBytesRead += _current.Size;

            if (_current.LastModified > DateTime.MinValue)
            {
                if (!_oldestLastModifiedUtc.HasValue || _current.LastModified < _oldestLastModifiedUtc.Value)
                    _oldestLastModifiedUtc = _current.LastModified;
                if (!_newestLastModifiedUtc.HasValue || _current.LastModified > _newestLastModifiedUtc.Value)
                    _newestLastModifiedUtc = _current.LastModified;
            }

            _storageClassCounts.TryGetValue(_current.StorageClass, out long scCount);
            _storageClassCounts[_current.StorageClass] = scCount + 1;

            return true;
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
