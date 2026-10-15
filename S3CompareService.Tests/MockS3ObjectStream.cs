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

    public S3ObjectSummary? Current => _index >= 0 && _index < _items.Count ? _items[_index] : null;

    public long TotalObjectsRead { get; private set; }
    public long TotalBytesRead { get; private set; }
    public bool HasMore => _index + 1 < _items.Count;

    public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        _index++;
        if (_index < _items.Count)
        {
            TotalObjectsRead++;
            TotalBytesRead += _items[_index].Size;
            return Task.FromResult(true);
        }
        return Task.FromResult(false);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
