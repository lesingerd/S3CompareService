using System.Diagnostics;
using Amazon.S3;
using Amazon.S3.Model;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Core.Engine;

public class ServiceComparisonEngine
{
    private readonly ComparisonJob _job;
    private readonly ScanRun _scanRun;
    private readonly IScanRunRepository _scanRunRepo;
    private readonly IDifferenceRepository _diffRepo;
    private readonly IAuditLogRepository? _auditLogRepo;
    private readonly IS3ObjectStream? _providedStreamA;
    private readonly IS3ObjectStream? _providedStreamB;
    private readonly ScanCheckpoint? _resumeCheckpoint;

    private readonly SequenceGapDetector? _gapDetector;
    private readonly ComparisonStatistics _stats = new();
    private readonly Stopwatch _stopwatch = new();
    private long _differenceIdCounter = 0;
    private string _lastEvaluatedKey = string.Empty;

    private readonly List<RecordedDifference> _differenceBuffer = new();
    private readonly SemaphoreSlim _bufferLock = new(1, 1);
    private long _lastBufferFlushTime = 0;

    private volatile bool _isPaused = false;
    private readonly SemaphoreSlim _pauseSemaphore = new(1, 1);

    public event Action<ComparisonStatistics>? ProgressUpdated;
    public event Action<RecordedDifference>? DifferenceFound;

    public ServiceComparisonEngine(
        ComparisonJob job,
        ScanRun scanRun,
        IScanRunRepository scanRunRepo,
        IDifferenceRepository diffRepo,
        IAuditLogRepository? auditLogRepo = null,
        ScanCheckpoint? resumeCheckpoint = null,
        IS3ObjectStream? streamA = null,
        IS3ObjectStream? streamB = null)
    {
        _job = job ?? throw new ArgumentNullException(nameof(job));
        _scanRun = scanRun ?? throw new ArgumentNullException(nameof(scanRun));
        _scanRunRepo = scanRunRepo ?? throw new ArgumentNullException(nameof(scanRunRepo));
        _diffRepo = diffRepo ?? throw new ArgumentNullException(nameof(diffRepo));
        _auditLogRepo = auditLogRepo;
        _resumeCheckpoint = resumeCheckpoint;
        _providedStreamA = streamA;
        _providedStreamB = streamB;

        if (_job.DetectSequenceGaps)
        {
            _gapDetector = new SequenceGapDetector(_job.MaxSequenceGapThreshold);
        }

        if (resumeCheckpoint != null)
        {
            InitializeStatsFromCheckpoint(resumeCheckpoint);
        }
    }

    private void InitializeStatsFromCheckpoint(ScanCheckpoint cp)
    {
        _stats.ObjectsCompared = cp.ObjectsCompared;
        _stats.MatchedCount = cp.MatchedCount;
        _stats.TotalDifferences = cp.TotalDifferences;
        _stats.FilteredDifferencesCount = cp.FilteredDifferencesCount;
        _stats.InFlightDifferencesCount = cp.InFlightDifferencesCount;
        _stats.MissingInBucketBCount = cp.MissingInBucketBCount;
        _stats.MissingInBucketACount = cp.MissingInBucketACount;
        _stats.SizeMismatchCount = cp.SizeMismatchCount;
        _stats.ETagMismatchCount = cp.ETagMismatchCount;
        _stats.ContentMismatchCount = cp.ContentMismatchCount;
        _stats.SequenceGapCount = cp.SequenceGapCount;
        _stats.ElapsedTime = TimeSpan.FromSeconds(cp.ElapsedSeconds);
        _lastEvaluatedKey = cp.LastEvaluatedKey;
        _differenceIdCounter = cp.TotalDifferences;
    }

    public void Pause()
    {
        if (!_isPaused)
        {
            _isPaused = true;
            _pauseSemaphore.WaitAsync();
        }
    }

    public void Resume()
    {
        if (_isPaused)
        {
            _isPaused = false;
            if (_pauseSemaphore.CurrentCount == 0)
            {
                _pauseSemaphore.Release();
            }
        }
    }

    public bool IsPaused => _isPaused;

    public async Task<ScanRun> RunAsync(CancellationToken cancellationToken = default)
    {
        _stopwatch.Restart();
        _scanRun.Status = RunStatus.Running;
        _scanRun.StartTimeUtc = DateTime.UtcNow;

        // Set up grace period window
        if (_job.ReplicationGracePeriodMinutes > 0)
        {
            _scanRun.ReplicationGracePeriodMinutes = _job.ReplicationGracePeriodMinutes;
            _scanRun.GracePeriodCutoffUtc = _scanRun.StartTimeUtc.AddMinutes(-_job.ReplicationGracePeriodMinutes);
            _scanRun.IsScanWindowClipped = true;
            _scanRun.ScanWindowNotes = $"Replication lag grace window active: objects modified after {_scanRun.GracePeriodCutoffUtc:yyyy-MM-dd HH:mm:ss} UTC ({_job.ReplicationGracePeriodMinutes} min) are flagged as in-flight replication.";
        }
        else
        {
            _scanRun.ReplicationGracePeriodMinutes = 0;
            _scanRun.GracePeriodCutoffUtc = null;
            _scanRun.IsScanWindowClipped = false;
            _scanRun.ScanWindowNotes = "Full scan window (no grace period exclusion).";
        }

        await _scanRunRepo.UpdateProgressAsync(_scanRun, cancellationToken).ConfigureAwait(false);
        if (_auditLogRepo != null)
        {
            await _auditLogRepo.LogAsync("INFO", "ScanEngine", $"Starting scan run '{_scanRun.Id}' for job '{_job.Name}' (Grace Period: {_job.ReplicationGracePeriodMinutes}m)", scanRunId: _scanRun.Id, jobId: _job.Id, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        IAmazonS3? clientA = null;
        IAmazonS3? clientB = null;
        IS3ObjectStream? enumA = null;
        IS3ObjectStream? enumB = null;

        try
        {
            if (_providedStreamA != null && _providedStreamB != null)
            {
                enumA = _providedStreamA;
                enumB = _providedStreamB;
            }
            else
            {
                clientA = S3ClientFactory.CreateClient(_job.BucketA);
                clientB = S3ClientFactory.CreateClient(_job.BucketB);

                string? startAfter = _resumeCheckpoint?.LastEvaluatedKey;

                enumA = new S3PagingEnumerator(
                    clientA,
                    _job.BucketA.BucketName,
                    _job.BucketA.Prefix,
                    startAfter);

                enumB = new S3PagingEnumerator(
                    clientB,
                    _job.BucketB.BucketName,
                    _job.BucketB.Prefix,
                    startAfter);
            }

            bool hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
            bool hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);

            long lastProgressReportTime = 0;
            long lastCheckpointTime = 0;
            long itemsSinceLastCheckpoint = 0;

            while (hasA && hasB)
            {
                await CheckPauseAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                var itemA = enumA.Current!;
                var itemB = enumB.Current!;

                _stats.CurrentKeyA = itemA.Key;
                _stats.CurrentKeyB = itemB.Key;

                int cmp = string.CompareOrdinal(itemA.Key, itemB.Key);

                if (cmp == 0)
                {
                    _stats.ObjectsCompared++;
                    _lastEvaluatedKey = itemA.Key;

                    CheckSequenceGap(itemA.Key);
                    await CompareSameKeyAsync(clientA, clientB, itemA, itemB, cancellationToken).ConfigureAwait(false);

                    hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                    hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                }
                else if (cmp < 0)
                {
                    // itemA exists in Bucket A but missing in Bucket B
                    _stats.ObjectsCompared++;
                    _lastEvaluatedKey = itemA.Key;

                    CheckSequenceGap(itemA.Key);

                    var diff = CreateDifference(
                        itemA.Key,
                        DifferenceType.MissingInBucketB,
                        $"Object exists in Bucket A but is missing in Bucket B. Size: {itemA.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}",
                        bucketASize: itemA.Size,
                        bucketBSize: null,
                        bucketAETag: itemA.ETag,
                        bucketBETag: null,
                        bucketALastModified: itemA.LastModified,
                        bucketBLastModified: null,
                        bucketAStorageClass: itemA.StorageClass,
                        bucketBStorageClass: null);

                    await BufferDifferenceAsync(diff, cancellationToken).ConfigureAwait(false);
                    _stats.MissingInBucketBCount++;

                    hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    // itemB exists in Bucket B but missing in Bucket A
                    _stats.ObjectsCompared++;
                    _lastEvaluatedKey = itemB.Key;

                    CheckSequenceGap(itemB.Key);

                    var diff = CreateDifference(
                        itemB.Key,
                        DifferenceType.MissingInBucketA,
                        $"Object exists in Bucket B but is missing in Bucket A. Size: {itemB.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}",
                        bucketASize: null,
                        bucketBSize: itemB.Size,
                        bucketAETag: null,
                        bucketBETag: itemB.ETag,
                        bucketALastModified: null,
                        bucketBLastModified: itemB.LastModified,
                        bucketAStorageClass: null,
                        bucketBStorageClass: itemB.StorageClass);

                    await BufferDifferenceAsync(diff, cancellationToken).ConfigureAwait(false);
                    _stats.MissingInBucketACount++;

                    hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                }

                UpdateLiveStats(enumA, enumB);
                itemsSinceLastCheckpoint++;

                long elapsedMs = _stopwatch.ElapsedMilliseconds;
                if (elapsedMs - lastCheckpointTime > 5000 || itemsSinceLastCheckpoint >= 5000)
                {
                    lastCheckpointTime = elapsedMs;
                    itemsSinceLastCheckpoint = 0;
                    await FlushDifferencesAsync(cancellationToken).ConfigureAwait(false);
                    await SaveCheckpointAsync(cancellationToken).ConfigureAwait(false);
                }

                if (elapsedMs - lastProgressReportTime > 250)
                {
                    lastProgressReportTime = elapsedMs;
                    ProgressUpdated?.Invoke(CloneStats());
                }
            }

            // Drain remaining items in Bucket A
            while (hasA)
            {
                await CheckPauseAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                var itemA = enumA.Current!;
                _stats.CurrentKeyA = itemA.Key;
                _stats.ObjectsCompared++;
                _lastEvaluatedKey = itemA.Key;

                CheckSequenceGap(itemA.Key);

                var diff = CreateDifference(
                    itemA.Key,
                    DifferenceType.MissingInBucketB,
                    $"Object exists in Bucket A only (Tail). Size: {itemA.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}",
                    bucketASize: itemA.Size,
                    bucketBSize: null,
                    bucketAETag: itemA.ETag,
                    bucketBETag: null,
                    bucketALastModified: itemA.LastModified,
                    bucketBLastModified: null,
                    bucketAStorageClass: itemA.StorageClass,
                    bucketBStorageClass: null);

                await BufferDifferenceAsync(diff, cancellationToken).ConfigureAwait(false);
                _stats.MissingInBucketBCount++;

                hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                UpdateLiveStats(enumA, enumB);
            }

            // Drain remaining items in Bucket B
            while (hasB)
            {
                await CheckPauseAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                var itemB = enumB.Current!;
                _stats.CurrentKeyB = itemB.Key;
                _stats.ObjectsCompared++;
                _lastEvaluatedKey = itemB.Key;

                CheckSequenceGap(itemB.Key);

                var diff = CreateDifference(
                    itemB.Key,
                    DifferenceType.MissingInBucketA,
                    $"Object exists in Bucket B only (Tail). Size: {itemB.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}",
                    bucketASize: null,
                    bucketBSize: itemB.Size,
                    bucketAETag: null,
                    bucketBETag: itemB.ETag,
                    bucketALastModified: null,
                    bucketBLastModified: itemB.LastModified,
                    bucketAStorageClass: null,
                    bucketBStorageClass: itemB.StorageClass);

                await BufferDifferenceAsync(diff, cancellationToken).ConfigureAwait(false);
                _stats.MissingInBucketACount++;

                hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                UpdateLiveStats(enumA, enumB);
            }

            // Flush any remaining differences
            await FlushDifferencesAsync(cancellationToken).ConfigureAwait(false);

            // Record Bucket Metadata Snapshots
            await RecordBucketMetadataSnapshotsAsync(enumA, enumB, cancellationToken).ConfigureAwait(false);

            // Scan completed normally
            _scanRun.Status = RunStatus.Completed;
            _scanRun.EndTimeUtc = DateTime.UtcNow;
            _scanRun.LastEvaluatedKey = _lastEvaluatedKey;
            SyncStatsToRun(_scanRun);

            await _scanRunRepo.CompleteAsync(_scanRun, cancellationToken).ConfigureAwait(false);
            await _scanRunRepo.DeleteCheckpointAsync(_scanRun.Id, cancellationToken).ConfigureAwait(false);

            if (_auditLogRepo != null)
            {
                await _auditLogRepo.LogAsync(
                    "INFO",
                    "ScanEngine",
                    $"Scan run '{_scanRun.Id}' completed successfully: {_scanRun.ObjectsCompared:N0} objects evaluated, {_scanRun.MatchedCount:N0} matched, {_scanRun.TotalDifferences:N0} raw diffs ({_scanRun.FilteredDifferencesCount:N0} confirmed disparities, {_scanRun.InFlightDifferencesCount:N0} in-flight replication).",
                    scanRunId: _scanRun.Id,
                    jobId: _job.Id,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            _scanRun.Status = RunStatus.Cancelled;
            _scanRun.EndTimeUtc = DateTime.UtcNow;
            _scanRun.ErrorMessage = "Scan was cancelled by user or service shutdown.";
            SyncStatsToRun(_scanRun);
            await FlushDifferencesAsync(CancellationToken.None).ConfigureAwait(false);
            await SaveCheckpointAsync(CancellationToken.None).ConfigureAwait(false);
            await _scanRunRepo.CompleteAsync(_scanRun, CancellationToken.None).ConfigureAwait(false);

            if (_auditLogRepo != null)
            {
                await _auditLogRepo.LogAsync("WARN", "ScanEngine", $"Scan run '{_scanRun.Id}' cancelled. Checkpoint saved at key '{_lastEvaluatedKey}'.", scanRunId: _scanRun.Id, jobId: _job.Id).ConfigureAwait(false);
            }
            throw;
        }
        catch (Exception ex)
        {
            _scanRun.Status = RunStatus.Failed;
            _scanRun.EndTimeUtc = DateTime.UtcNow;
            _scanRun.ErrorMessage = $"{ex.GetType().Name}: {ex.Message}";
            SyncStatsToRun(_scanRun);
            await FlushDifferencesAsync(CancellationToken.None).ConfigureAwait(false);
            await SaveCheckpointAsync(CancellationToken.None).ConfigureAwait(false);
            await _scanRunRepo.CompleteAsync(_scanRun, CancellationToken.None).ConfigureAwait(false);

            if (_auditLogRepo != null)
            {
                await _auditLogRepo.LogAsync("ERROR", "ScanEngine", $"Scan run '{_scanRun.Id}' failed with error: {ex.Message}", details: ex.StackTrace, scanRunId: _scanRun.Id, jobId: _job.Id).ConfigureAwait(false);
            }
            throw;
        }
        finally
        {
            _stopwatch.Stop();
            if (enumA != null) await enumA.DisposeAsync().ConfigureAwait(false);
            if (enumB != null) await enumB.DisposeAsync().ConfigureAwait(false);
            clientA?.Dispose();
            clientB?.Dispose();
        }

        UpdateLiveStats(null, null);
        ProgressUpdated?.Invoke(CloneStats());
        return _scanRun;
    }

    private RecordedDifference CreateDifference(
        string key,
        DifferenceType type,
        string details,
        long? bucketASize,
        long? bucketBSize,
        string? bucketAETag,
        string? bucketBETag,
        DateTime? bucketALastModified,
        DateTime? bucketBLastModified,
        string? bucketAStorageClass,
        string? bucketBStorageClass)
    {
        bool isWithinGrace = false;
        if (_scanRun.GracePeriodCutoffUtc.HasValue)
        {
            var cutoff = _scanRun.GracePeriodCutoffUtc.Value;
            if ((bucketALastModified.HasValue && bucketALastModified.Value >= cutoff) ||
                (bucketBLastModified.HasValue && bucketBLastModified.Value >= cutoff))
            {
                isWithinGrace = true;
            }
        }

        var diff = new RecordedDifference
        {
            Id = ++_differenceIdCounter,
            ScanRunId = _scanRun.Id,
            Key = key,
            Type = type,
            Details = details,
            BucketASize = bucketASize,
            BucketBSize = bucketBSize,
            BucketAETag = bucketAETag,
            BucketBETag = bucketBETag,
            BucketALastModified = bucketALastModified,
            BucketBLastModified = bucketBLastModified,
            BucketAStorageClass = bucketAStorageClass,
            BucketBStorageClass = bucketBStorageClass,
            DetectedAtUtc = DateTime.UtcNow,
            IsWithinGracePeriod = isWithinGrace,
            GracePeriodCutoffUtc = _scanRun.GracePeriodCutoffUtc
        };

        if (isWithinGrace)
        {
            _stats.InFlightDifferencesCount++;
        }
        else
        {
            _stats.FilteredDifferencesCount++;
        }
        _stats.TotalDifferences++;

        return diff;
    }

    private async Task BufferDifferenceAsync(RecordedDifference diff, CancellationToken cancellationToken)
    {
        DifferenceFound?.Invoke(diff);

        await _bufferLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _differenceBuffer.Add(diff);
            long elapsed = _stopwatch.ElapsedMilliseconds;
            if (_differenceBuffer.Count >= 500 || elapsed - _lastBufferFlushTime > 5000)
            {
                _lastBufferFlushTime = elapsed;
                var copy = _differenceBuffer.ToList();
                _differenceBuffer.Clear();
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await _diffRepo.InsertBatchAsync(copy, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Batch insert retry/safe fallback
                    }
                }, CancellationToken.None);
            }
        }
        finally
        {
            _bufferLock.Release();
        }
    }

    private async Task FlushDifferencesAsync(CancellationToken cancellationToken)
    {
        await _bufferLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_differenceBuffer.Count > 0)
            {
                var copy = _differenceBuffer.ToList();
                _differenceBuffer.Clear();
                await _diffRepo.InsertBatchAsync(copy, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _bufferLock.Release();
        }
    }

    private async Task SaveCheckpointAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_lastEvaluatedKey)) return;

        try
        {
            var cp = new ScanCheckpoint
            {
                ScanRunId = _scanRun.Id,
                JobId = _job.Id,
                CreatedAtUtc = _scanRun.StartTimeUtc,
                LastUpdatedAtUtc = DateTime.UtcNow,
                BucketNameA = _job.BucketA.BucketName,
                EndpointA = _job.BucketA.EndpointUrl,
                PrefixA = _job.BucketA.Prefix,
                BucketNameB = _job.BucketB.BucketName,
                EndpointB = _job.BucketB.EndpointUrl,
                PrefixB = _job.BucketB.Prefix,
                Mode = _job.Mode,
                LastEvaluatedKey = _lastEvaluatedKey,
                ObjectsCompared = _stats.ObjectsCompared,
                MatchedCount = _stats.MatchedCount,
                TotalDifferences = _stats.TotalDifferences,
                FilteredDifferencesCount = _stats.FilteredDifferencesCount,
                InFlightDifferencesCount = _stats.InFlightDifferencesCount,
                MissingInBucketBCount = _stats.MissingInBucketBCount,
                MissingInBucketACount = _stats.MissingInBucketACount,
                SizeMismatchCount = _stats.SizeMismatchCount,
                ETagMismatchCount = _stats.ETagMismatchCount,
                ContentMismatchCount = _stats.ContentMismatchCount,
                SequenceGapCount = _stats.SequenceGapCount,
                ElapsedSeconds = _stopwatch.Elapsed.TotalSeconds,
                DetectSequenceGaps = _job.DetectSequenceGaps,
                MaxSequenceGapThreshold = _job.MaxSequenceGapThreshold
            };

            await _scanRunRepo.SaveCheckpointAsync(cp, cancellationToken).ConfigureAwait(false);

            _scanRun.LastEvaluatedKey = _lastEvaluatedKey;
            SyncStatsToRun(_scanRun);
            await _scanRunRepo.UpdateProgressAsync(_scanRun, cancellationToken).ConfigureAwait(false);

            _stats.LastCheckpointKey = _lastEvaluatedKey;
            _stats.LastCheckpointTime = DateTime.UtcNow;
        }
        catch
        {
            // Do not break scan on checkpoint write failure
        }
    }

    private async Task RecordBucketMetadataSnapshotsAsync(IS3ObjectStream? enumA, IS3ObjectStream? enumB, CancellationToken cancellationToken)
    {
        try
        {
            var snapA = new BucketMetadataSnapshot
            {
                ScanRunId = _scanRun.Id,
                BucketSide = "A",
                BucketName = _job.BucketA.BucketName,
                EndpointUrl = _job.BucketA.EndpointUrl,
                Region = _job.BucketA.Region,
                Prefix = _job.BucketA.Prefix,
                SnapshotTimeUtc = DateTime.UtcNow,
                TotalObjects = enumA?.TotalObjectsRead ?? _stats.BucketAObjectsRead,
                TotalBytes = enumA?.TotalBytesRead ?? _stats.BucketABytesRead
            };

            var snapB = new BucketMetadataSnapshot
            {
                ScanRunId = _scanRun.Id,
                BucketSide = "B",
                BucketName = _job.BucketB.BucketName,
                EndpointUrl = _job.BucketB.EndpointUrl,
                Region = _job.BucketB.Region,
                Prefix = _job.BucketB.Prefix,
                SnapshotTimeUtc = DateTime.UtcNow,
                TotalObjects = enumB?.TotalObjectsRead ?? _stats.BucketBObjectsRead,
                TotalBytes = enumB?.TotalBytesRead ?? _stats.BucketBBytesRead
            };

            await _scanRunRepo.SaveMetadataSnapshotAsync(snapA, cancellationToken).ConfigureAwait(false);
            await _scanRunRepo.SaveMetadataSnapshotAsync(snapB, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Non-critical
        }
    }

    private void CheckSequenceGap(string key)
    {
        if (_gapDetector == null) return;

        var gaps = _gapDetector.CheckKey(key, _scanRun.Id, ref _differenceIdCounter);
        foreach (var gap in gaps)
        {
            _stats.SequenceGapCount++;
            _stats.FilteredDifferencesCount++;
            _stats.TotalDifferences++;
            _ = BufferDifferenceAsync(gap, CancellationToken.None);
        }
    }

    private async Task CompareSameKeyAsync(
        IAmazonS3? clientA,
        IAmazonS3? clientB,
        S3ObjectSummary itemA,
        S3ObjectSummary itemB,
        CancellationToken cancellationToken)
    {
        // 1. Size Check
        if (itemA.Size != itemB.Size)
        {
            var diff = CreateDifference(
                itemA.Key,
                DifferenceType.SizeMismatch,
                $"Size mismatch: A has {itemA.FormattedSize} ({itemA.Size} B), B has {itemB.FormattedSize} ({itemB.Size} B)",
                bucketASize: itemA.Size,
                bucketBSize: itemB.Size,
                bucketAETag: itemA.ETag,
                bucketBETag: itemB.ETag,
                bucketALastModified: itemA.LastModified,
                bucketBLastModified: itemB.LastModified,
                bucketAStorageClass: itemA.StorageClass,
                bucketBStorageClass: itemB.StorageClass);

            await BufferDifferenceAsync(diff, cancellationToken).ConfigureAwait(false);
            _stats.SizeMismatchCount++;
            return;
        }

        // 2. ETag Check
        bool etagsIdentical = string.Equals(itemA.ETag, itemB.ETag, StringComparison.OrdinalIgnoreCase);
        if (etagsIdentical)
        {
            _stats.MatchedCount++;
            return;
        }

        // 3. ETags Differ
        if (_job.Mode == ComparisonMode.ListingOnly || clientA == null || clientB == null)
        {
            var diff = CreateDifference(
                itemA.Key,
                DifferenceType.ETagMismatch,
                $"ETags differ (A: {itemA.ETag}, B: {itemB.ETag}). Size is identical ({itemA.FormattedSize}). Note: multipart uploads have different ETags even if content matches.",
                bucketASize: itemA.Size,
                bucketBSize: itemB.Size,
                bucketAETag: itemA.ETag,
                bucketBETag: itemB.ETag,
                bucketALastModified: itemA.LastModified,
                bucketBLastModified: itemB.LastModified,
                bucketAStorageClass: itemA.StorageClass,
                bucketBStorageClass: itemB.StorageClass);

            await BufferDifferenceAsync(diff, cancellationToken).ConfigureAwait(false);
            _stats.ETagMismatchCount++;
        }
        else
        {
            // Full Data stream compare
            bool contentMatches = await StreamCompareObjectsAsync(clientA, clientB, itemA.Key, cancellationToken).ConfigureAwait(false);
            if (contentMatches)
            {
                _stats.MatchedCount++;
            }
            else
            {
                var diff = CreateDifference(
                    itemA.Key,
                    DifferenceType.ContentMismatch,
                    $"Content byte mismatch! Size is identical ({itemA.FormattedSize}) but data stream differs.",
                    bucketASize: itemA.Size,
                    bucketBSize: itemB.Size,
                    bucketAETag: itemA.ETag,
                    bucketBETag: itemB.ETag,
                    bucketALastModified: itemA.LastModified,
                    bucketBLastModified: itemB.LastModified,
                    bucketAStorageClass: itemA.StorageClass,
                    bucketBStorageClass: itemB.StorageClass);

                await BufferDifferenceAsync(diff, cancellationToken).ConfigureAwait(false);
                _stats.ContentMismatchCount++;
            }
        }
    }

    private async Task<bool> StreamCompareObjectsAsync(
        IAmazonS3 clientA,
        IAmazonS3 clientB,
        string key,
        CancellationToken cancellationToken)
    {
        var getA = new GetObjectRequest { BucketName = _job.BucketA.BucketName, Key = key };
        var getB = new GetObjectRequest { BucketName = _job.BucketB.BucketName, Key = key };

        using var respA = await clientA.GetObjectAsync(getA, cancellationToken).ConfigureAwait(false);
        using var respB = await clientB.GetObjectAsync(getB, cancellationToken).ConfigureAwait(false);

        await using var streamA = respA.ResponseStream;
        await using var streamB = respB.ResponseStream;

        byte[] bufA = new byte[65536];
        byte[] bufB = new byte[65536];

        while (true)
        {
            int bytesReadA = await ReadFullBufferAsync(streamA, bufA, cancellationToken).ConfigureAwait(false);
            int bytesReadB = await ReadFullBufferAsync(streamB, bufB, cancellationToken).ConfigureAwait(false);

            if (bytesReadA != bytesReadB)
                return false;

            if (bytesReadA == 0)
                return true;

            if (!bufA.AsSpan(0, bytesReadA).SequenceEqual(bufB.AsSpan(0, bytesReadB)))
                return false;
        }
    }

    private static async Task<int> ReadFullBufferAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken).ConfigureAwait(false);
            if (read == 0)
                break;
            totalRead += read;
        }
        return totalRead;
    }

    private async Task CheckPauseAsync(CancellationToken cancellationToken)
    {
        if (_isPaused)
        {
            await _pauseSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            _pauseSemaphore.Release();
        }
    }

    private void UpdateLiveStats(IS3ObjectStream? enumA, IS3ObjectStream? enumB)
    {
        double totalSeconds = _stopwatch.Elapsed.TotalSeconds;
        if (_resumeCheckpoint != null)
        {
            totalSeconds += _resumeCheckpoint.ElapsedSeconds;
        }
        _stats.ElapsedTime = TimeSpan.FromSeconds(totalSeconds);
        _stats.ItemsPerSecond = totalSeconds > 0 ? _stats.ObjectsCompared / totalSeconds : 0;

        var proc = Process.GetCurrentProcess();
        _stats.WorkingSetMB = proc.WorkingSet64 / (1024.0 * 1024.0);
        _stats.GCTotalMemoryMB = GC.GetTotalMemory(false) / (1024.0 * 1024.0);

        if (enumA != null)
        {
            _stats.BucketAObjectsRead = enumA.TotalObjectsRead;
            _stats.BucketABytesRead = enumA.TotalBytesRead;
        }
        if (enumB != null)
        {
            _stats.BucketBObjectsRead = enumB.TotalObjectsRead;
            _stats.BucketBBytesRead = enumB.TotalBytesRead;
        }
    }

    private void SyncStatsToRun(ScanRun run)
    {
        run.ObjectsCompared = _stats.ObjectsCompared;
        run.MatchedCount = _stats.MatchedCount;
        run.TotalDifferences = _stats.TotalDifferences;
        run.FilteredDifferencesCount = _stats.FilteredDifferencesCount;
        run.InFlightDifferencesCount = _stats.InFlightDifferencesCount;
        run.MissingInBucketBCount = _stats.MissingInBucketBCount;
        run.MissingInBucketACount = _stats.MissingInBucketACount;
        run.SizeMismatchCount = _stats.SizeMismatchCount;
        run.ETagMismatchCount = _stats.ETagMismatchCount;
        run.ContentMismatchCount = _stats.ContentMismatchCount;
        run.SequenceGapCount = _stats.SequenceGapCount;
        run.BucketAObjectsRead = _stats.BucketAObjectsRead;
        run.BucketBObjectsRead = _stats.BucketBObjectsRead;
        run.BucketABytesRead = _stats.BucketABytesRead;
        run.BucketBBytesRead = _stats.BucketBBytesRead;
        run.DurationSeconds = _stopwatch.Elapsed.TotalSeconds;
    }

    private ComparisonStatistics CloneStats()
    {
        return new ComparisonStatistics
        {
            ObjectsCompared = _stats.ObjectsCompared,
            MatchedCount = _stats.MatchedCount,
            TotalDifferences = _stats.TotalDifferences,
            FilteredDifferencesCount = _stats.FilteredDifferencesCount,
            InFlightDifferencesCount = _stats.InFlightDifferencesCount,
            MissingInBucketBCount = _stats.MissingInBucketBCount,
            MissingInBucketACount = _stats.MissingInBucketACount,
            SizeMismatchCount = _stats.SizeMismatchCount,
            ETagMismatchCount = _stats.ETagMismatchCount,
            ContentMismatchCount = _stats.ContentMismatchCount,
            SequenceGapCount = _stats.SequenceGapCount,
            BucketAObjectsRead = _stats.BucketAObjectsRead,
            BucketBObjectsRead = _stats.BucketBObjectsRead,
            BucketABytesRead = _stats.BucketABytesRead,
            BucketBBytesRead = _stats.BucketBBytesRead,
            ElapsedTime = _stats.ElapsedTime,
            ItemsPerSecond = _stats.ItemsPerSecond,
            CurrentKeyA = _stats.CurrentKeyA,
            CurrentKeyB = _stats.CurrentKeyB,
            WorkingSetMB = _stats.WorkingSetMB,
            GCTotalMemoryMB = _stats.GCTotalMemoryMB,
            LastCheckpointKey = _stats.LastCheckpointKey,
            LastCheckpointTime = _stats.LastCheckpointTime
        };
    }
}
