using System.Diagnostics;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;

namespace S3CompareService.Core.Engine;

/// <summary>
/// Streaming, checkpointed comparison of two S3 buckets.
///
/// Resume contract: a checkpoint is only written after every difference up to its LastEvaluatedKey has been
/// persisted, and LastEvaluatedKey / counters only advance once a key has been completely processed. To resume,
/// pass the *same* ScanRun row (see <see cref="IScanRunRepository.ReopenForResumeAsync"/>) and its checkpoint
/// (<see cref="IScanRunRepository.GetCheckpointAsync"/>) and, when not using the real S3 enumerators, streams that
/// already start after the checkpoint key. Differences then stay under the original run id.
/// </summary>
public class ServiceComparisonEngine
{
    private const int DifferenceBatchSize = 500;
    private const int MaxConsecutiveCheckpointFailures = 5;

    private readonly ComparisonJob _job;
    private readonly ScanRun _scanRun;
    private readonly IScanRunRepository _scanRunRepo;
    private readonly IDifferenceRepository _diffRepo;
    private readonly IAuditLogRepository? _auditLogRepo;
    private readonly IS3ObjectStream? _providedStreamA;
    private readonly IS3ObjectStream? _providedStreamB;
    private readonly ScanCheckpoint? _resumeCheckpoint;
    private readonly bool _checkpointDiscarded;

    private readonly SequenceGapDetector? _gapDetector;
    private readonly ComparisonStatistics _stats = new();
    private readonly Stopwatch _stopwatch = new();
    private readonly double _baselineElapsedSeconds;
    private string _lastEvaluatedKey = string.Empty;
    private bool _hasLastKey;

    // Objects fully consumed from each side (excludes the look-ahead item the merge loop is holding).
    private long _processedA, _processedB, _processedBytesA, _processedBytesB;

    private readonly List<RecordedDifference> _differenceBuffer = new();
    private readonly object _bufferLock = new();
    private long _lastBufferFlushTime = 0;
    private int _consecutiveCheckpointFailures;
    private int _handlerFailureLogged;

    private readonly object _pauseLock = new();
    private volatile bool _isPaused = false;
    private volatile bool _isServiceShutdown = false;
    private TaskCompletionSource<bool>? _pauseTcs;
    private Task _statusPersistTask = Task.CompletedTask;

    public event Action<ComparisonStatistics>? ProgressUpdated;
    public event Action<RecordedDifference>? DifferenceFound;

    /// <summary>Why a supplied checkpoint was ignored (job configuration changed), or null.</summary>
    public string? CheckpointDiscardReason { get; }

    /// <summary>True when the engine will continue from a valid checkpoint.</summary>
    public bool IsResuming => _resumeCheckpoint != null;

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
        _providedStreamA = streamA;
        _providedStreamB = streamB;

        if (_job.DetectSequenceGaps)
        {
            _gapDetector = new SequenceGapDetector(_job.MaxSequenceGapThreshold);
        }

        if (resumeCheckpoint != null)
        {
            CheckpointDiscardReason = ValidateCheckpoint(resumeCheckpoint);
            if (CheckpointDiscardReason == null)
            {
                _resumeCheckpoint = resumeCheckpoint;
                _baselineElapsedSeconds = resumeCheckpoint.ElapsedSeconds;
                InitializeStatsFromCheckpoint(resumeCheckpoint);
                _scanRun.IsResumedFromCheckpoint = true;
                _scanRun.ResumedFromKey ??= resumeCheckpoint.LastEvaluatedKey;
            }
            else
            {
                _checkpointDiscarded = true;
            }
        }
    }

    private string? ValidateCheckpoint(ScanCheckpoint cp)
    {
        if (!cp.HasKey) return "checkpoint has no evaluated key";
        if (!string.IsNullOrEmpty(cp.JobId) && !string.Equals(cp.JobId, _job.Id, StringComparison.Ordinal))
            return $"checkpoint belongs to job '{cp.JobId}', not '{_job.Id}'";
        if (!string.IsNullOrEmpty(cp.BucketNameA) && cp.BucketNameA != _job.BucketA.BucketName)
            return "bucket A name changed";
        if (!string.IsNullOrEmpty(cp.BucketNameB) && cp.BucketNameB != _job.BucketB.BucketName)
            return "bucket B name changed";
        if (!string.IsNullOrEmpty(cp.EndpointA) && cp.EndpointA != _job.BucketA.EndpointUrl)
            return "bucket A endpoint changed";
        if (!string.IsNullOrEmpty(cp.EndpointB) && cp.EndpointB != _job.BucketB.EndpointUrl)
            return "bucket B endpoint changed";
        if ((cp.PrefixA ?? string.Empty) != (_job.BucketA.Prefix ?? string.Empty))
            return "bucket A prefix changed";
        if ((cp.PrefixB ?? string.Empty) != (_job.BucketB.Prefix ?? string.Empty))
            return "bucket B prefix changed";
        if (cp.Mode != _job.Mode)
            return "comparison mode changed";
        return null;
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
        _stats.BucketAObjectsRead = _processedA = cp.BucketAObjectsRead;
        _stats.BucketBObjectsRead = _processedB = cp.BucketBObjectsRead;
        _stats.BucketABytesRead = _processedBytesA = cp.BucketABytesRead;
        _stats.BucketBBytesRead = _processedBytesB = cp.BucketBBytesRead;
        _stats.ElapsedTime = TimeSpan.FromSeconds(cp.ElapsedSeconds);
        _lastEvaluatedKey = cp.LastEvaluatedKey ?? string.Empty;
        _hasLastKey = cp.HasKey;
    }

    public void Pause()
    {
        lock (_pauseLock)
        {
            if (!_isPaused)
            {
                _isPaused = true;
                _stopwatch.Stop();
                _pauseTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                _scanRun.Status = RunStatus.Paused;
                QueueStatusPersist();
            }
        }
    }

    public void Resume()
    {
        lock (_pauseLock)
        {
            if (_isPaused)
            {
                _isPaused = false;
                _stopwatch.Start();
                _scanRun.Status = RunStatus.Running;
                _pauseTcs?.TrySetResult(true);
                _pauseTcs = null;
                QueueStatusPersist();
            }
        }
    }

    // Status writes triggered by Pause/Resume are chained so they execute in order, their failures are observed,
    // and RunAsync can await them before writing the final state (a late "Running" must not overwrite "Completed").
    private void QueueStatusPersist()
    {
        _statusPersistTask = _statusPersistTask.ContinueWith(async _ =>
        {
            try
            {
                await _scanRunRepo.UpdateProgressAsync(_scanRun, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await LogAsync("WARN", $"Failed to persist run status '{_scanRun.Status}': {ex.Message}").ConfigureAwait(false);
            }
        }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default).Unwrap();
    }

    public void MarkServiceShutdown()
    {
        _isServiceShutdown = true;
        Resume();
    }

    public bool IsPaused => _isPaused;

    private static string GetRelativeKey(string fullKey, string? prefix)
    {
        if (string.IsNullOrEmpty(prefix))
            return fullKey;
        return fullKey.StartsWith(prefix, StringComparison.Ordinal)
            ? fullKey.Substring(prefix.Length)
            : fullKey;
    }

    /// <summary>Checkpoint keys are always relative to the bucket prefix, so the S3 start-after key is prefix + relative key.</summary>
    public static string? BuildStartAfterKey(bool hasKey, string? relativeKey, string? prefix)
    {
        if (!hasKey) return null;
        string full = (prefix ?? string.Empty) + (relativeKey ?? string.Empty);
        return full.Length == 0 ? null : full;
    }

    private async Task LogAsync(string level, string message, string? details = null)
    {
        if (_auditLogRepo == null) return;
        try
        {
            await _auditLogRepo.LogAsync(level, "ScanEngine", message, details: details, scanRunId: _scanRun.Id, jobId: _job.Id, cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // audit logging must never break a scan
        }
    }

    private void ApplyGraceWindow()
    {
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
    }

    public async Task<ScanRun> RunAsync(CancellationToken cancellationToken = default)
    {
        _stopwatch.Restart();
        _scanRun.Status = RunStatus.Running;

        // Resuming into the same run keeps the original start time (and therefore the same grace cutoff) so
        // differences classified before and after the interruption are consistent.
        bool resumingSameRun = _resumeCheckpoint != null && _resumeCheckpoint.ScanRunId == _scanRun.Id;
        if (!resumingSameRun)
        {
            _scanRun.StartTimeUtc = DateTime.UtcNow;
        }
        ApplyGraceWindow();

        IAmazonS3? clientA = null;
        IAmazonS3? clientB = null;
        IS3ObjectStream? enumA = null;
        IS3ObjectStream? enumB = null;

        string? prefixA = string.IsNullOrEmpty(_job.BucketA.Prefix) ? null : _job.BucketA.Prefix;
        string? prefixB = string.IsNullOrEmpty(_job.BucketB.Prefix) ? null : _job.BucketB.Prefix;

        try
        {
            await PrepareRunStateAsync(cancellationToken).ConfigureAwait(false);

            await _scanRunRepo.UpdateProgressAsync(_scanRun, cancellationToken).ConfigureAwait(false);
            if (_checkpointDiscarded)
            {
                await LogAsync("WARN", $"Checkpoint for run '{_scanRun.Id}' was discarded ({CheckpointDiscardReason}); starting a fresh scan.").ConfigureAwait(false);
            }
            await LogAsync("INFO", $"Starting scan run '{_scanRun.Id}' for job '{_job.Name}' (Grace Period: {_job.ReplicationGracePeriodMinutes}m){(IsResuming ? $", resuming after key '{_lastEvaluatedKey}'" : string.Empty)}").ConfigureAwait(false);

            if (_providedStreamA != null && _providedStreamB != null)
            {
                enumA = _providedStreamA;
                enumB = _providedStreamB;
            }
            else
            {
                clientA = S3ClientFactory.CreateClient(_job.BucketA);
                clientB = S3ClientFactory.CreateClient(_job.BucketB);

                bool hasKey = IsResuming && _hasLastKey;
                string? startAfterA = BuildStartAfterKey(hasKey, _lastEvaluatedKey, prefixA);
                string? startAfterB = BuildStartAfterKey(hasKey, _lastEvaluatedKey, prefixB);

                enumA = new S3PagingEnumerator(
                    clientA,
                    _job.BucketA.BucketName,
                    prefixA ?? string.Empty,
                    startAfterA,
                    initialObjectsRead: _processedA,
                    initialBytesRead: _processedBytesA);

                enumB = new S3PagingEnumerator(
                    clientB,
                    _job.BucketB.BucketName,
                    prefixB ?? string.Empty,
                    startAfterB,
                    initialObjectsRead: _processedB,
                    initialBytesRead: _processedBytesB);
            }

            bool hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
            bool hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);

            var maintenance = new MaintenanceState();

            while (hasA || hasB)
            {
                await CheckPauseAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                S3ObjectSummary? itemA = hasA ? enumA.Current : null;
                S3ObjectSummary? itemB = hasB ? enumB.Current : null;
                string? relKeyA = itemA != null ? GetRelativeKey(itemA.Key, prefixA) : null;
                string? relKeyB = itemB != null ? GetRelativeKey(itemB.Key, prefixB) : null;

                int cmp;
                if (itemA == null) cmp = 1;
                else if (itemB == null) cmp = -1;
                else cmp = Utf8OrdinalComparer.CompareStatic(relKeyA, relKeyB);

                // Phase 1: everything that can be slow or cancelled (S3 data compare) - mutates no state.
                // Phase 2 (CommitKey): synchronous bookkeeping - the key is either fully recorded or not at all.
                if (cmp == 0)
                {
                    var (matched, diff) = await CompareSameKeyAsync(clientA, clientB, relKeyA!, itemA!, itemB!, cancellationToken).ConfigureAwait(false);
                    CommitKey(relKeyA!, itemA, itemB, matched, diff);
                    hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                    hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                }
                else if (cmp < 0)
                {
                    bool tail = itemB == null;
                    var diff = MakeDifference(
                        relKeyA!,
                        DifferenceType.MissingInBucketB,
                        tail
                            ? $"Object exists in Bucket A only (Tail). Size: {itemA!.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}"
                            : $"Object exists in Bucket A but is missing in Bucket B. Size: {itemA!.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}",
                        itemA, null);
                    CommitKey(relKeyA!, itemA, null, false, diff);
                    hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    bool tail = itemA == null;
                    var diff = MakeDifference(
                        relKeyB!,
                        DifferenceType.MissingInBucketA,
                        tail
                            ? $"Object exists in Bucket B only (Tail). Size: {itemB!.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}"
                            : $"Object exists in Bucket B but is missing in Bucket A. Size: {itemB!.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}",
                        null, itemB);
                    CommitKey(relKeyB!, null, itemB, false, diff);
                    hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                }

                await PeriodicMaintenanceStepAsync(maintenance, cancellationToken).ConfigureAwait(false);
            }

            // Flush any remaining differences
            await FlushDifferencesAsync(cancellationToken).ConfigureAwait(false);

            // Record Bucket Metadata Snapshots
            await RecordBucketMetadataSnapshotsAsync(clientA, clientB, enumA, enumB, cancellationToken).ConfigureAwait(false);

            // Scan completed normally
            UpdateLiveStats(includeSystemDiagnostics: true);
            _scanRun.Status = RunStatus.Completed;
            _scanRun.EndTimeUtc = DateTime.UtcNow;
            _scanRun.ErrorMessage = null;
            _scanRun.LastEvaluatedKey = _lastEvaluatedKey;
            SyncStatsToRun(_scanRun);

            await ObserveStatusPersistAsync().ConfigureAwait(false);
            await _scanRunRepo.CompleteAsync(_scanRun, cancellationToken).ConfigureAwait(false);
            await _scanRunRepo.DeleteCheckpointAsync(_scanRun.Id, cancellationToken).ConfigureAwait(false);

            await LogAsync("INFO", $"Scan run '{_scanRun.Id}' completed successfully: {_scanRun.ObjectsCompared:N0} objects evaluated, {_scanRun.MatchedCount:N0} matched, {_scanRun.TotalDifferences:N0} raw diffs ({_scanRun.FilteredDifferencesCount:N0} confirmed disparities, {_scanRun.InFlightDifferencesCount:N0} in-flight replication).").ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            var status = _isServiceShutdown ? RunStatus.Interrupted : RunStatus.Cancelled;
            string message = _isServiceShutdown
                ? "Scan interrupted by service shutdown. Checkpoint saved for automatic recovery."
                : "Scan was cancelled by user.";
            await FinalizeRunAsync(status, message).ConfigureAwait(false);

            await LogAsync(
                _isServiceShutdown ? "RECOVERY" : "WARN",
                $"Scan run '{_scanRun.Id}' {(_isServiceShutdown ? "interrupted for service shutdown" : "cancelled")}. Checkpoint saved at key '{_lastEvaluatedKey}'.").ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            await FinalizeRunAsync(RunStatus.Failed, $"{ex.GetType().Name}: {ex.Message}").ConfigureAwait(false);
            await LogAsync("ERROR", $"Scan run '{_scanRun.Id}' failed with error: {ex.Message}", ex.StackTrace).ConfigureAwait(false);
            throw;
        }
        finally
        {
            _stopwatch.Stop();
            try { if (enumA != null) await enumA.DisposeAsync().ConfigureAwait(false); } catch { }
            try { if (enumB != null) await enumB.DisposeAsync().ConfigureAwait(false); } catch { }
            try { clientA?.Dispose(); } catch { }
            try { clientB?.Dispose(); } catch { }
        }

        UpdateLiveStats(includeSystemDiagnostics: true);
        RaiseProgress();
        return _scanRun;
    }

    /// <summary>Brings the database in line with how this engine run starts (resume vs. fresh).</summary>
    private async Task PrepareRunStateAsync(CancellationToken ct)
    {
        if (_resumeCheckpoint != null)
        {
            // Drops differences flushed after the checkpoint (they will be recreated) and, for a legacy
            // "new run id" resume, adopts the old run's differences and marks the old run superseded.
            int removed = await _scanRunRepo.PrepareResumeAsync(_scanRun.Id, _resumeCheckpoint.ScanRunId, _lastEvaluatedKey, ct).ConfigureAwait(false);
            if (removed > 0)
            {
                await LogAsync("INFO", $"Resume: removed {removed} difference(s) recorded after checkpoint key '{_lastEvaluatedKey}' to avoid duplicates.").ConfigureAwait(false);
            }
        }
        else if (_scanRun.ResumeCount > 0 || _scanRun.IsResumedFromCheckpoint || _checkpointDiscarded)
        {
            // Re-opened run without a usable checkpoint: restart from scratch under the same id.
            await _scanRunRepo.ResetRunDataAsync(_scanRun.Id, ct).ConfigureAwait(false);
            _scanRun.IsResumedFromCheckpoint = false;
            _scanRun.ResumedFromKey = null;
            _scanRun.LastEvaluatedKey = string.Empty;
            SyncStatsToRun(_scanRun);
        }
    }

    private async Task ObserveStatusPersistAsync()
    {
        Task t;
        lock (_pauseLock) { t = _statusPersistTask; }
        try { await t.ConfigureAwait(false); } catch { }
    }

    /// <summary>
    /// Writes the terminal state for a cancelled/interrupted/failed run. Never throws: every step is guarded so
    /// CompleteAsync always gets its chance, and the checkpoint is only written if all differences were flushed.
    /// </summary>
    private async Task FinalizeRunAsync(RunStatus status, string message)
    {
        try { UpdateLiveStats(includeSystemDiagnostics: true); } catch { }

        _scanRun.Status = status;
        _scanRun.EndTimeUtc = DateTime.UtcNow;
        _scanRun.ErrorMessage = message;
        _scanRun.LastEvaluatedKey = _lastEvaluatedKey;
        SyncStatsToRun(_scanRun);

        bool flushed = false;
        try
        {
            await FlushDifferencesAsync(CancellationToken.None).ConfigureAwait(false);
            flushed = true;
        }
        catch (Exception ex)
        {
            int lost;
            lock (_bufferLock) { lost = _differenceBuffer.Count; }
            _scanRun.ErrorMessage = $"{message} WARNING: {lost} difference(s) could not be persisted ({ex.Message}); the checkpoint was not advanced.";
            await LogAsync("ERROR", $"Final difference flush failed for run '{_scanRun.Id}': {ex.Message}", ex.StackTrace).ConfigureAwait(false);
        }

        if (flushed)
        {
            await SaveCheckpointAsync(CancellationToken.None, throwOnRepeatedFailure: false).ConfigureAwait(false);
        }

        await ObserveStatusPersistAsync().ConfigureAwait(false);

        try
        {
            await _scanRunRepo.CompleteAsync(_scanRun, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await LogAsync("ERROR", $"Failed to write final state of run '{_scanRun.Id}': {ex.Message}", ex.StackTrace).ConfigureAwait(false);
        }
    }

    private sealed class MaintenanceState
    {
        public long LastProgressReportTime;
        public long LastCheckpointTime;
        public long ItemsSinceLastCheckpoint;
    }

    private async Task PeriodicMaintenanceStepAsync(MaintenanceState m, CancellationToken cancellationToken)
    {
        UpdateLiveStats(includeSystemDiagnostics: false);
        m.ItemsSinceLastCheckpoint++;

        long elapsedMs = _stopwatch.ElapsedMilliseconds;

        int buffered;
        lock (_bufferLock) { buffered = _differenceBuffer.Count; }
        if (buffered >= DifferenceBatchSize || (buffered > 0 && elapsedMs - _lastBufferFlushTime > 5000))
        {
            await FlushDifferencesAsync(cancellationToken).ConfigureAwait(false);
        }

        if (elapsedMs - m.LastCheckpointTime > 5000 || m.ItemsSinceLastCheckpoint >= 5000)
        {
            m.LastCheckpointTime = elapsedMs;
            m.ItemsSinceLastCheckpoint = 0;
            UpdateLiveStats(includeSystemDiagnostics: true);
            await FlushDifferencesAsync(cancellationToken).ConfigureAwait(false);
            await SaveCheckpointAsync(cancellationToken, throwOnRepeatedFailure: true).ConfigureAwait(false);
        }

        if (elapsedMs - m.LastProgressReportTime > 250)
        {
            m.LastProgressReportTime = elapsedMs;
            UpdateLiveStats(includeSystemDiagnostics: true);
            RaiseProgress();
        }
    }

    private void RaiseProgress()
    {
        var handler = ProgressUpdated;
        if (handler == null) return;
        try { handler(CloneStats()); }
        catch (Exception ex) { ReportHandlerFailure("ProgressUpdated", ex); }
    }

    private void RaiseDifferenceFound(RecordedDifference diff)
    {
        var handler = DifferenceFound;
        if (handler == null) return;
        try { handler(diff); }
        catch (Exception ex) { ReportHandlerFailure("DifferenceFound", ex); }
    }

    private void ReportHandlerFailure(string eventName, Exception ex)
    {
        // Subscriber bugs must never fail a scan; log the first one only.
        if (Interlocked.Exchange(ref _handlerFailureLogged, 1) == 0)
        {
            _ = LogAsync("WARN", $"Event handler for {eventName} threw and was ignored: {ex.Message}", ex.StackTrace);
        }
    }

    private RecordedDifference MakeDifference(string key, DifferenceType type, string details, S3ObjectSummary? a, S3ObjectSummary? b)
    {
        bool isWithinGrace = false;
        if (_scanRun.GracePeriodCutoffUtc.HasValue)
        {
            var cutoff = _scanRun.GracePeriodCutoffUtc.Value;
            if ((a != null && a.LastModified >= cutoff) || (b != null && b.LastModified >= cutoff))
            {
                isWithinGrace = true;
            }
        }

        // Id is intentionally left 0: the database assigns it on insert.
        return new RecordedDifference
        {
            ScanRunId = _scanRun.Id,
            Key = key,
            Type = type,
            Details = details,
            BucketASize = a?.Size,
            BucketBSize = b?.Size,
            BucketAETag = a?.ETag,
            BucketBETag = b?.ETag,
            BucketALastModified = a?.LastModified,
            BucketBLastModified = b?.LastModified,
            BucketAStorageClass = a?.StorageClass,
            BucketBStorageClass = b?.StorageClass,
            DetectedAtUtc = DateTime.UtcNow,
            IsWithinGracePeriod = isWithinGrace,
            GracePeriodCutoffUtc = _scanRun.GracePeriodCutoffUtc
        };
    }

    private void ApplyDifferenceStats(RecordedDifference diff)
    {
        if (diff.IsWithinGracePeriod)
        {
            _stats.InFlightDifferencesCount++;
        }
        else
        {
            _stats.FilteredDifferencesCount++;
            switch (diff.Type)
            {
                case DifferenceType.MissingInBucketB: _stats.MissingInBucketBCount++; break;
                case DifferenceType.MissingInBucketA: _stats.MissingInBucketACount++; break;
                case DifferenceType.SizeMismatch: _stats.SizeMismatchCount++; break;
                case DifferenceType.ETagMismatch: _stats.ETagMismatchCount++; break;
                case DifferenceType.ContentMismatch: _stats.ContentMismatchCount++; break;
                case DifferenceType.PossibleMissingOnBothSides: _stats.SequenceGapCount++; break;
            }
        }
        _stats.TotalDifferences++;
    }

    /// <summary>
    /// Atomically (no awaits, no cancellation points) records the outcome of one key: counters, buffered
    /// differences, consumed-object tallies and finally the evaluated-key marker.
    /// </summary>
    private void CommitKey(string relKey, S3ObjectSummary? a, S3ObjectSummary? b, bool matched, RecordedDifference? diff)
    {
        var toRecord = new List<RecordedDifference>(2);

        if (_gapDetector != null)
        {
            long unusedIdCounter = 0; // detector API legacy; ids are assigned by the database
            toRecord.AddRange(_gapDetector.CheckKey(relKey, _scanRun.Id, ref unusedIdCounter));
        }
        if (diff != null) toRecord.Add(diff);

        _stats.ObjectsCompared++;
        if (matched) _stats.MatchedCount++;
        if (a != null) { _processedA++; _processedBytesA += a.Size; _stats.CurrentKeyA = a.Key; }
        if (b != null) { _processedB++; _processedBytesB += b.Size; _stats.CurrentKeyB = b.Key; }

        lock (_bufferLock)
        {
            foreach (var d in toRecord)
            {
                ApplyDifferenceStats(d);
                _differenceBuffer.Add(d);
            }
        }

        _lastEvaluatedKey = relKey;
        _hasLastKey = true;

        foreach (var d in toRecord) RaiseDifferenceFound(d);
    }

    /// <summary>
    /// Persists buffered differences. Entries are removed from the buffer only after the insert succeeded, so a
    /// failed flush loses nothing and can be retried; the failure propagates to the caller.
    /// </summary>
    private async Task FlushDifferencesAsync(CancellationToken cancellationToken)
    {
        List<RecordedDifference> batch;
        lock (_bufferLock)
        {
            if (_differenceBuffer.Count == 0) return;
            batch = _differenceBuffer.ToList();
        }

        await InsertBatchWithRetryAsync(batch, cancellationToken).ConfigureAwait(false);

        lock (_bufferLock)
        {
            // Only this loop appends (to the end), so the first batch.Count entries are exactly what we inserted.
            _differenceBuffer.RemoveRange(0, batch.Count);
        }
        _lastBufferFlushTime = _stopwatch.ElapsedMilliseconds;
    }

    private async Task InsertBatchWithRetryAsync(List<RecordedDifference> batch, CancellationToken cancellationToken)
    {
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                await _diffRepo.InsertBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < 3 && !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(100 * attempt, cancellationToken).ConfigureAwait(false);
                if (attempt == 2)
                {
                    await LogAsync("WARN", $"Retrying difference batch insert ({batch.Count} items) after error: {ex.Message}").ConfigureAwait(false);
                }
            }
        }
    }

    private async Task<bool> SaveCheckpointAsync(CancellationToken cancellationToken, bool throwOnRepeatedFailure)
    {
        if (!_hasLastKey) return true; // nothing processed yet - nothing to resume from

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
                PrefixA = _job.BucketA.Prefix ?? string.Empty,
                BucketNameB = _job.BucketB.BucketName,
                EndpointB = _job.BucketB.EndpointUrl,
                PrefixB = _job.BucketB.Prefix ?? string.Empty,
                Mode = _job.Mode,
                LastEvaluatedKey = _lastEvaluatedKey,
                HasLastEvaluatedKey = true,
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
                ElapsedSeconds = _stats.ElapsedTime.TotalSeconds,
                DetectSequenceGaps = _job.DetectSequenceGaps,
                MaxSequenceGapThreshold = _job.MaxSequenceGapThreshold
            };

            await _scanRunRepo.SaveCheckpointAsync(cp, cancellationToken).ConfigureAwait(false);

            _scanRun.LastEvaluatedKey = _lastEvaluatedKey;
            SyncStatsToRun(_scanRun);
            await _scanRunRepo.UpdateProgressAsync(_scanRun, cancellationToken).ConfigureAwait(false);

            _stats.LastCheckpointKey = _lastEvaluatedKey;
            _stats.LastCheckpointTime = DateTime.UtcNow;
            _consecutiveCheckpointFailures = 0;
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _consecutiveCheckpointFailures++;
            await LogAsync("WARN", $"Checkpoint write failed ({_consecutiveCheckpointFailures} consecutive): {ex.Message}", ex.StackTrace).ConfigureAwait(false);

            if (throwOnRepeatedFailure && _consecutiveCheckpointFailures >= MaxConsecutiveCheckpointFailures)
            {
                throw new InvalidOperationException(
                    $"Checkpoint persistence failed {_consecutiveCheckpointFailures} consecutive times; aborting the scan because it could not be resumed safely. Last error: {ex.Message}", ex);
            }
            return false;
        }
    }

    private async Task RecordBucketMetadataSnapshotsAsync(
        IAmazonS3? clientA,
        IAmazonS3? clientB,
        IS3ObjectStream? enumA,
        IS3ObjectStream? enumB,
        CancellationToken cancellationToken)
    {
        try
        {
            string? versioningA = null;
            string? versioningB = null;
            if (clientA != null && !string.IsNullOrWhiteSpace(_job.BucketA.BucketName))
            {
                try
                {
                    var verRespA = await clientA.GetBucketVersioningAsync(new GetBucketVersioningRequest { BucketName = _job.BucketA.BucketName }, cancellationToken).ConfigureAwait(false);
                    versioningA = verRespA.VersioningConfig?.Status?.Value ?? "Disabled";
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch { versioningA = "Unknown"; }
            }
            if (clientB != null && !string.IsNullOrWhiteSpace(_job.BucketB.BucketName))
            {
                try
                {
                    var verRespB = await clientB.GetBucketVersioningAsync(new GetBucketVersioningRequest { BucketName = _job.BucketB.BucketName }, cancellationToken).ConfigureAwait(false);
                    versioningB = verRespB.VersioningConfig?.Status?.Value ?? "Disabled";
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch { versioningB = "Unknown"; }
            }

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
                TotalBytes = enumA?.TotalBytesRead ?? _stats.BucketABytesRead,
                VersioningStatus = versioningA,
                StorageClassesJson = enumA != null ? JsonSerializer.Serialize(enumA.StorageClassCounts) : "{}",
                OldestLastModifiedUtc = enumA?.OldestLastModifiedUtc,
                NewestLastModifiedUtc = enumA?.NewestLastModifiedUtc
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
                TotalBytes = enumB?.TotalBytesRead ?? _stats.BucketBBytesRead,
                VersioningStatus = versioningB,
                StorageClassesJson = enumB != null ? JsonSerializer.Serialize(enumB.StorageClassCounts) : "{}",
                OldestLastModifiedUtc = enumB?.OldestLastModifiedUtc,
                NewestLastModifiedUtc = enumB?.NewestLastModifiedUtc
            };

            await _scanRunRepo.SaveMetadataSnapshotAsync(snapA, cancellationToken).ConfigureAwait(false);
            await _scanRunRepo.SaveMetadataSnapshotAsync(snapB, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Non-critical for the scan result, but never silent.
            await LogAsync("WARN", $"Failed to record bucket metadata snapshots for run '{_scanRun.Id}': {ex.Message}", ex.StackTrace).ConfigureAwait(false);
        }
    }

    private async Task<(bool Matched, RecordedDifference? Diff)> CompareSameKeyAsync(
        IAmazonS3? clientA,
        IAmazonS3? clientB,
        string relativeKey,
        S3ObjectSummary itemA,
        S3ObjectSummary itemB,
        CancellationToken cancellationToken)
    {
        // 1. Size Check
        if (itemA.Size != itemB.Size)
        {
            return (false, MakeDifference(
                relativeKey,
                DifferenceType.SizeMismatch,
                $"Size mismatch: A has {itemA.FormattedSize} ({itemA.Size} B), B has {itemB.FormattedSize} ({itemB.Size} B)",
                itemA, itemB));
        }

        // 2. ETag Check
        if (string.Equals(itemA.ETag, itemB.ETag, StringComparison.OrdinalIgnoreCase))
        {
            return (true, null);
        }

        // 3. ETags Differ
        if (_job.Mode == ComparisonMode.ListingOnly || clientA == null || clientB == null)
        {
            return (false, MakeDifference(
                relativeKey,
                DifferenceType.ETagMismatch,
                $"ETags differ (A: {itemA.ETag}, B: {itemB.ETag}). Size is identical ({itemA.FormattedSize}). Note: multipart uploads have different ETags even if content matches.",
                itemA, itemB));
        }

        // Full Data stream compare using each bucket's full key
        bool contentMatches = await StreamCompareObjectsAsync(clientA, clientB, itemA.Key, itemB.Key, cancellationToken).ConfigureAwait(false);
        if (contentMatches)
        {
            return (true, null);
        }

        return (false, MakeDifference(
            relativeKey,
            DifferenceType.ContentMismatch,
            $"Content byte mismatch! Size is identical ({itemA.FormattedSize}) but data stream differs.",
            itemA, itemB));
    }

    private async Task<bool> StreamCompareObjectsAsync(
        IAmazonS3 clientA,
        IAmazonS3 clientB,
        string keyA,
        string keyB,
        CancellationToken cancellationToken)
    {
        var getA = new GetObjectRequest { BucketName = _job.BucketA.BucketName, Key = keyA };
        var getB = new GetObjectRequest { BucketName = _job.BucketB.BucketName, Key = keyB };

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
        Task? waitTask = null;
        lock (_pauseLock)
        {
            if (_isPaused && _pauseTcs != null)
            {
                waitTask = _pauseTcs.Task;
            }
        }

        if (waitTask != null)
        {
            await waitTask.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private void UpdateLiveStats(bool includeSystemDiagnostics = false)
    {
        double totalSeconds = _stopwatch.Elapsed.TotalSeconds + _baselineElapsedSeconds;
        _stats.ElapsedTime = TimeSpan.FromSeconds(totalSeconds);
        _stats.ItemsPerSecond = totalSeconds > 0 ? _stats.ObjectsCompared / totalSeconds : 0;

        if (includeSystemDiagnostics)
        {
            _stats.WorkingSetMB = Environment.WorkingSet / (1024.0 * 1024.0);
            _stats.GCTotalMemoryMB = GC.GetTotalMemory(false) / (1024.0 * 1024.0);
        }

        // Only objects the merge loop has fully consumed - never the look-ahead item - so a resume that
        // re-reads from the checkpoint key does not double count.
        _stats.BucketAObjectsRead = _processedA;
        _stats.BucketBObjectsRead = _processedB;
        _stats.BucketABytesRead = _processedBytesA;
        _stats.BucketBBytesRead = _processedBytesB;
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
        run.DurationSeconds = _stats.ElapsedTime.TotalSeconds;
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
