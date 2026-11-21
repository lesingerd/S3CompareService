using System.Collections.Concurrent;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;

namespace S3CompareService.Tests;

/// <summary>
/// In-memory <see cref="IS3Lister"/> with ListObjectsV2 semantics: keys in UTF-8 order, Prefix, StartAfter (first request),
/// opaque continuation tokens, Delimiter (common prefixes; MaxKeys counts objects and prefixes), MaxKeys. Optional artificial
/// latency per call and fault injection by call number. Thread-safe.
/// </summary>
public sealed class FakeS3Lister : IS3Lister
{
    private readonly List<S3ObjectSummary> _items;
    private readonly List<string> _keys;
    private int _calls;
    private int _inFlight;
    private int _maxInFlight;

    public FakeS3Lister(IEnumerable<S3ObjectSummary> items, TimeSpan? latency = null)
    {
        _items = items.OrderBy(o => o.Key, Utf8OrdinalComparer.Instance).ToList();
        _keys = _items.Select(o => o.Key).ToList();
        Latency = latency ?? TimeSpan.Zero;
    }

    public TimeSpan Latency { get; set; }

    /// <summary>Called with the 1-based call number before answering; return an exception to fail that call.</summary>
    public Func<int, Exception?>? Fault { get; set; }

    /// <summary>Called with the 1-based call number when a call starts (before the latency).</summary>
    public Action<int>? OnCall { get; set; }

    public int Calls => Volatile.Read(ref _calls);
    public int MaxInFlight => Volatile.Read(ref _maxInFlight);
    public ConcurrentQueue<(string? Prefix, string? StartAfter, string? Token, string? Delimiter, int MaxKeys)> Requests { get; } = new();

    public async Task<S3ListPage> ListPageAsync(string bucket, string? prefix, string? startAfter, string? continuationToken, string? delimiter, int maxKeys, CancellationToken cancellationToken)
    {
        int call = Interlocked.Increment(ref _calls);
        Requests.Enqueue((prefix, startAfter, continuationToken, delimiter, maxKeys));
        int now = Interlocked.Increment(ref _inFlight);
        int seen;
        while (now > (seen = Volatile.Read(ref _maxInFlight)) && Interlocked.CompareExchange(ref _maxInFlight, now, seen) != seen) { }
        try
        {
            OnCall?.Invoke(call);
            if (Latency > TimeSpan.Zero) await Task.Delay(Latency, cancellationToken).ConfigureAwait(false);
            else await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            var fault = Fault?.Invoke(call);
            if (fault != null) throw fault;
            return Page(prefix ?? string.Empty, startAfter, continuationToken, delimiter, Math.Clamp(maxKeys, 1, 1000));
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    private int FirstIndexAfter(string? key)
    {
        if (key == null) return 0;
        int lo = 0, hi = _keys.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            if (Utf8OrdinalComparer.CompareStatic(_keys[mid], key) <= 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private int FirstIndexAtOrAfter(string key)
    {
        int lo = 0, hi = _keys.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            if (Utf8OrdinalComparer.CompareStatic(_keys[mid], key) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private S3ListPage Page(string prefix, string? startAfter, string? token, string? delimiter, int maxKeys)
    {
        string? after = startAfter;
        string? skipPrefix = null;
        if (token != null)
        {
            after = token.Substring(2);
            if (token.StartsWith("C:", StringComparison.Ordinal)) skipPrefix = after;
        }

        int start = Math.Max(FirstIndexAfter(after), FirstIndexAtOrAfter(prefix));
        var objects = new List<S3ObjectSummary>();
        var prefixes = new List<string>();
        string? last = null;
        bool truncated = false;
        int count = 0;

        for (int i = start; i < _items.Count; i++)
        {
            string key = _keys[i];
            if (!key.StartsWith(prefix, StringComparison.Ordinal))
            {
                if (Utf8OrdinalComparer.CompareStatic(key, prefix) > 0) break; // past the prefix block
                continue;
            }
            if (skipPrefix != null && key.StartsWith(skipPrefix, StringComparison.Ordinal)) continue;

            string? commonPrefix = null;
            if (!string.IsNullOrEmpty(delimiter))
            {
                int idx = key.IndexOf(delimiter, prefix.Length, StringComparison.Ordinal);
                if (idx >= 0) commonPrefix = key.Substring(0, idx + delimiter.Length);
            }

            if (count == maxKeys)
            {
                truncated = true;
                break;
            }

            if (commonPrefix != null)
            {
                prefixes.Add(commonPrefix);
                skipPrefix = commonPrefix;
                last = "C:" + commonPrefix;
            }
            else
            {
                objects.Add(_items[i]);
                last = "K:" + key;
            }
            count++;
        }

        return new S3ListPage
        {
            Objects = objects,
            CommonPrefixes = prefixes,
            IsTruncated = truncated,
            NextContinuationToken = truncated ? last : null
        };
    }
}
