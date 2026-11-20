using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

/// <summary>
/// A bucket listing that runs ahead of its consumer. A background producer requests page N+1 as soon as page N has arrived and
/// puts the pages into a bounded channel (capacity <c>prefetchPages</c>), so the comparison loop normally finds the next page
/// already waiting instead of paying one round trip per 1000 keys. ListObjectsV2 pages of one listing stay strictly sequential
/// (the continuation token is opaque); the gain is that listing and comparing overlap, and that bucket A and bucket B list at the
/// same time.
///
/// <list type="bullet">
/// <item>Every request waits for the bucket's rate limiter first (shared with all other listings of the scan).</item>
/// <item>The listing stops at the first key whose key relative to <c>prefix</c> sorts after <c>upperRelativeInclusive</c>
/// (UTF-8 order), so a key-range partition never reads into the next range beyond the last page.</item>
/// <item>A producer exception (S3 error, throttling, ...) is rethrown to the consumer by its next <see cref="MoveNextAsync"/>,
/// after the pages that arrived before it.</item>
/// <item><see cref="DisposeAsync"/> cancels the producer and waits for it, so no request outlives the stream.</item>
/// <item>Statistics count consumed objects only (not prefetched ones), like <see cref="S3PagingEnumerator"/>.</item>
/// </list>
/// </summary>
public sealed class PrefetchingObjectStream : IS3ObjectStream
{
    private readonly IS3Lister _lister;
    private readonly string _bucket;
    private readonly string _prefix;
    private readonly string? _startAfter;
    private readonly string? _upper;
    private readonly IRequestRateLimiter? _rateLimiter;
    private readonly int _pageSize;
    private readonly Channel<List<S3ObjectSummary>> _pages;
    private readonly CancellationTokenSource _cts;
    private readonly Task _producer;
    private volatile Exception? _producerError;
    private int _pagesRequested;

    private List<S3ObjectSummary>? _page;
    private int _index;
    private bool _finished;
    private S3ObjectSummary? _current;
    private long _totalObjectsRead;
    private long _totalBytesRead;
    private DateTime? _oldestLastModifiedUtc;
    private DateTime? _newestLastModifiedUtc;
    private readonly Dictionary<string, long> _storageClassCounts = new(StringComparer.OrdinalIgnoreCase);
    private int _disposed;

    /// <param name="lister">Lists one page.</param>
    /// <param name="bucket">Bucket name.</param>
    /// <param name="prefix">The bucket prefix (used as the list Prefix and to compute relative keys).</param>
    /// <param name="startAfter">Full key to start after (null = from the start of the prefix).</param>
    /// <param name="upperRelativeInclusive">Stop after this relative key (null = list to the end).</param>
    /// <param name="prefetchPages">Pages buffered ahead of the consumer (at least 1).</param>
    /// <param name="rateLimiter">Waited on before every request, or null.</param>
    /// <param name="pageSize">MaxKeys per request.</param>
    /// <param name="cancellationToken">Stops the producer (in addition to <see cref="DisposeAsync"/>).</param>
    public PrefetchingObjectStream(
        IS3Lister lister,
        string bucket,
        string? prefix,
        string? startAfter,
        string? upperRelativeInclusive,
        int prefetchPages,
        IRequestRateLimiter? rateLimiter,
        int pageSize = 1000,
        CancellationToken cancellationToken = default)
    {
        _lister = lister ?? throw new ArgumentNullException(nameof(lister));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _prefix = prefix ?? string.Empty;
        _startAfter = string.IsNullOrEmpty(startAfter) ? null : startAfter;
        _upper = upperRelativeInclusive;
        _rateLimiter = rateLimiter;
        _pageSize = Math.Clamp(pageSize, 1, 1000);
        _pages = Channel.CreateBounded<List<S3ObjectSummary>>(new BoundedChannelOptions(Math.Max(1, prefetchPages))
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = _cts.Token;
        _producer = Task.Run(() => ProduceAsync(token));
    }

    public S3ObjectSummary? Current => _current;
    public long TotalObjectsRead => _totalObjectsRead;
    public long TotalBytesRead => _totalBytesRead;
    public DateTime? OldestLastModifiedUtc => _oldestLastModifiedUtc;
    public DateTime? NewestLastModifiedUtc => _newestLastModifiedUtc;
    public IReadOnlyDictionary<string, long> StorageClassCounts => _storageClassCounts;
    public bool HasMore => !_finished;

    /// <summary>List requests sent so far (diagnostics and tests).</summary>
    public int PagesRequested => Volatile.Read(ref _pagesRequested);

    private string RelativeKey(string fullKey) =>
        _prefix.Length > 0 && fullKey.StartsWith(_prefix, StringComparison.Ordinal) ? fullKey.Substring(_prefix.Length) : fullKey;

    private async Task ProduceAsync(CancellationToken ct)
    {
        try
        {
            string? token = null;
            bool first = true;
            while (true)
            {
                if (_rateLimiter != null) await _rateLimiter.WaitAsync(ct).ConfigureAwait(false);
                var page = await _lister.ListPageAsync(_bucket, _prefix, first ? _startAfter : null, token, null, _pageSize, ct).ConfigureAwait(false);
                first = false;
                Interlocked.Increment(ref _pagesRequested);

                if (page.IsTruncated && string.IsNullOrEmpty(page.NextContinuationToken))
                {
                    throw new InvalidOperationException(
                        $"S3 listing of bucket '{_bucket}' reported IsTruncated=true but returned no NextContinuationToken; refusing to loop. The endpoint may not fully support ListObjectsV2 pagination.");
                }

                var items = page.Objects ?? new List<S3ObjectSummary>();
                bool reachedUpper = false;
                if (_upper != null)
                {
                    int cut = items.FindIndex(o => Utf8OrdinalComparer.CompareStatic(RelativeKey(o.Key), _upper) > 0);
                    if (cut >= 0)
                    {
                        items = items.GetRange(0, cut);
                        reachedUpper = true;
                    }
                }

                if (items.Count > 0) await _pages.Writer.WriteAsync(items, ct).ConfigureAwait(false);
                if (reachedUpper || !page.IsTruncated) break;
                token = page.NextContinuationToken;
            }
        }
        catch (Exception ex)
        {
            // Includes cancellation; the consumer sees it after the pages that were already delivered.
            _producerError = ex;
        }
        finally
        {
            _pages.Writer.TryComplete();
        }
    }

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        while (_page == null || _index >= _page.Count)
        {
            if (_finished)
            {
                _current = null;
                return false;
            }

            if (_pages.Reader.TryRead(out var next))
            {
                _page = next;
                _index = 0;
                continue;
            }

            if (!await _pages.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var error = _producerError;
                if (error != null) ExceptionDispatchInfo.Capture(error).Throw();
                _finished = true;
                _current = null;
                return false;
            }
        }

        _current = _page[_index++];

        // Counted on consumption (not on page load), so checkpoints never include prefetched objects.
        _totalObjectsRead++;
        _totalBytesRead += _current.Size;
        if (_current.LastModified > DateTime.MinValue)
        {
            if (!_oldestLastModifiedUtc.HasValue || _current.LastModified < _oldestLastModifiedUtc.Value)
                _oldestLastModifiedUtc = _current.LastModified;
            if (!_newestLastModifiedUtc.HasValue || _current.LastModified > _newestLastModifiedUtc.Value)
                _newestLastModifiedUtc = _current.LastModified;
        }
        string sc = string.IsNullOrWhiteSpace(_current.StorageClass) ? "STANDARD" : _current.StorageClass;
        _storageClassCounts.TryGetValue(sc, out long count);
        _storageClassCounts[sc] = count + 1;
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try { _cts.Cancel(); } catch (ObjectDisposedException) { }
        try { await _producer.ConfigureAwait(false); } catch { /* the producer stores its errors; nothing to observe */ }
        _cts.Dispose();
        _page = null;
    }
}
