using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;

namespace S3CompareService.Tests;

public class MockS3ObjectStream : IS3ObjectStream
{
    private readonly List<S3ObjectSummary> _items;
    private readonly Action<long>? _onMoveNext;
    private int _index = -1;

    /// <param name="items">Items; sorted in UTF-8 (S3) key order.</param>
    /// <param name="startAfter">If set, only keys after it are returned (simulates S3 StartAfter on resume).</param>
    /// <param name="onMoveNext">Invoked with the 1-based count of items handed out so far, before each item is returned.</param>
    public MockS3ObjectStream(IEnumerable<S3ObjectSummary> items, string? startAfter = null, Action<long>? onMoveNext = null)
    {
        _items = items
            .Where(x => startAfter == null || Utf8OrdinalComparer.CompareStatic(x.Key, startAfter) > 0)
            .OrderBy(x => x.Key, Utf8OrdinalComparer.Instance)
            .ToList();
        _onMoveNext = onMoveNext;
    }

    private readonly Dictionary<string, long> _storageClassCounts = new(StringComparer.OrdinalIgnoreCase);

    public S3ObjectSummary? Current => _index >= 0 && _index < _items.Count ? _items[_index] : null;

    public long TotalObjectsRead { get; private set; }
    public long TotalBytesRead { get; private set; }
    public DateTime? OldestLastModifiedUtc { get; private set; }
    public DateTime? NewestLastModifiedUtc { get; private set; }
    public IReadOnlyDictionary<string, long> StorageClassCounts => _storageClassCounts;
    public bool HasMore => _index + 1 < _items.Count;

    public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        _index++;
        if (_index < _items.Count)
        {
            var item = _items[_index];
            TotalObjectsRead++;
            TotalBytesRead += item.Size;

            if (item.LastModified > DateTime.MinValue)
            {
                if (!OldestLastModifiedUtc.HasValue || item.LastModified < OldestLastModifiedUtc.Value)
                    OldestLastModifiedUtc = item.LastModified;
                if (!NewestLastModifiedUtc.HasValue || item.LastModified > NewestLastModifiedUtc.Value)
                    NewestLastModifiedUtc = item.LastModified;
            }

            string sc = string.IsNullOrWhiteSpace(item.StorageClass) ? "STANDARD" : item.StorageClass;
            _storageClassCounts.TryGetValue(sc, out long count);
            _storageClassCounts[sc] = count + 1;

            _onMoveNext?.Invoke(TotalObjectsRead);
            return Task.FromResult(true);
        }
        return Task.FromResult(false);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
