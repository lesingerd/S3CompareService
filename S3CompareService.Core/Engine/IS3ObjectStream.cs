using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

public interface IS3ObjectStream : IAsyncDisposable
{
    S3ObjectSummary? Current { get; }
    long TotalObjectsRead { get; }
    long TotalBytesRead { get; }
    bool HasMore { get; }
    Task<bool> MoveNextAsync(CancellationToken cancellationToken = default);
}
