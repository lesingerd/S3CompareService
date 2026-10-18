using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;

namespace S3CompareService.Tests;

public class MockS3ObjectStream : IS3ObjectStream
{
    private readonly List<S3ObjectSummary> _items;
    private int _index = -1;

    public MockS3ObjectStream(IEnumerable<S3ObjectSummary> items)
    {
        _items = items.OrderBy(x => x.Key, StringComparer.Ordinal).ToList();
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

            return Task.FromResult(true);
        }
        return Task.FromResult(false);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
