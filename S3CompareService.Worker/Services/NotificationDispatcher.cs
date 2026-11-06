using System.Diagnostics;
using S3CompareService.Core.Models;
using S3CompareService.Core.Notifications;
using S3CompareService.Core.Storage;

namespace S3CompareService.Worker.Services;

/// <summary>Where the scheduler and the scan coordinator hand notifications over. Both calls return at once and never wait for a delivery.</summary>
public interface INotificationQueue
{
    /// <summary>Queues a threshold or failure alert (or a periodic success digest) for a finished scan.</summary>
    void EnqueueAlert(ComparisonJob job, ScanRun run, List<string> reasons);

    /// <summary>Queues the daily digest of a job. Returns false when one is already queued, running or done for the same job and UTC day, or when the queue dropped it.</summary>
    bool TryEnqueueDigest(string jobId, DateTime nowUtc);
}

/// <summary>A point-in-time view of the notification queue and the last deliveries, for <c>/api/status</c>.</summary>
public sealed record NotificationStatus(
    int Queued,
    long Dropped,
    long Delivered,
    long Failed,
    DateTime? LastSuccessUtc,
    DateTime? LastFailureUtc,
    string? LastError,
    int ConsecutiveFailures,
    DateTime? FailingSinceUtc);

/// <summary>
/// Delivers alerts and daily digests on a task of its own, so that a slow or dead SMTP server, webhook or App Insights endpoint never
/// delays the scheduler loop or a scan. The queue is bounded: when it is full the oldest digest (or periodic success notice) is dropped
/// and counted; failure and threshold alerts are never dropped. Each delivery is bounded by <see cref="NotificationOptions"/> in the
/// alert service (one timeout per channel, abandoned when the channel ignores its token), and the whole dispatch has a backstop too.
/// Timestamps (last alert, last digest) are written only after a successful delivery. Nothing here can stop the host.
/// </summary>
public sealed class NotificationDispatcher : BackgroundService, INotificationQueue
{
    /// <summary>The most items waiting at once. Only digests and periodic success notices are dropped to stay under it.</summary>
    public const int Capacity = 100;

    /// <summary>How long a digest that failed to deliver waits before the scheduler may queue it again.</summary>
    public static readonly TimeSpan DigestRetryDelay = TimeSpan.FromMinutes(5);

    private abstract record Item(bool Droppable);
    private sealed record AlertItem(ComparisonJob Job, ScanRun Run, List<string> Reasons, bool Critical) : Item(!Critical);
    private sealed record DigestItem(string JobId, string Key) : Item(true);

    private readonly IJobRepository _jobs;
    private readonly IScanRunRepository _runs;
    private readonly IAlertNotificationService _alerts;
    private readonly ILogger<NotificationDispatcher> _logger;
    private readonly TimeSpan _backstop;

    private readonly object _gate = new();
    private readonly LinkedList<Item> _queue = new();
    private readonly SemaphoreSlim _signal = new(0);
    private readonly Dictionary<string, long> _digestKeys = new(); // key -> Stopwatch timestamp before which it may not be queued again
    private long _dropped, _delivered, _failed;
    private DateTime? _lastSuccess, _lastFailure, _failingSince;
    private string? _lastError;
    private int _consecutiveFailures;

    public NotificationDispatcher(IJobRepository jobs, IScanRunRepository runs, IAlertNotificationService alerts, NotificationOptions options, ILogger<NotificationDispatcher> logger)
    {
        _jobs = jobs;
        _runs = runs;
        _alerts = alerts;
        _logger = logger;
        _backstop = options.Timeout + TimeSpan.FromSeconds(5);
    }

    /// <inheritdoc />
    public void EnqueueAlert(ComparisonJob job, ScanRun run, List<string> reasons)
    {
        bool critical = run.Status == RunStatus.Failed || (reasons.Count > 0 && !reasons[0].StartsWith("Periodic status", StringComparison.Ordinal));
        Enqueue(new AlertItem(job, run, reasons, critical));
    }

    /// <inheritdoc />
    public bool TryEnqueueDigest(string jobId, DateTime nowUtc)
    {
        string day = nowUtc.ToString("yyyy-MM-dd");
        string key = $"{jobId}|{day}";
        long now = Stopwatch.GetTimestamp();
        lock (_gate)
        {
            foreach (var old in _digestKeys.Keys.Where(k => !k.EndsWith(day, StringComparison.Ordinal)).ToList()) _digestKeys.Remove(old);
            if (_digestKeys.TryGetValue(key, out var notBefore) && now < notBefore) return false;
            _digestKeys[key] = long.MaxValue; // queued or running
        }

        return Enqueue(new DigestItem(jobId, key));
    }

    private bool Enqueue(Item item)
    {
        lock (_gate)
        {
            if (_queue.Count >= Capacity)
            {
                var victim = _queue.First;
                while (victim != null && !victim.Value.Droppable) victim = victim.Next;
                if (victim != null)
                {
                    _queue.Remove(victim);
                    CountDrop(victim.Value);
                }
                else if (item.Droppable)
                {
                    CountDrop(item);
                    return false;
                }
                // The queue holds only failure alerts and this is one too: let it grow, those are never dropped.
            }

            _queue.AddLast(item);
        }

        _signal.Release();
        return true;
    }

    // Called with the lock held.
    private void CountDrop(Item dropped)
    {
        _dropped++;
        if (dropped is DigestItem d) _digestKeys.Remove(d.Key); // it can be queued again
        _logger.LogWarning("The notification queue is full ({Capacity}); dropped the oldest {Kind}. Dropped so far: {Dropped}.", Capacity, dropped is DigestItem ? "daily digest" : "status notice", _dropped);
    }

    /// <summary>Gets the queue length, the drop count and the last delivery results.</summary>
    public NotificationStatus GetStatus()
    {
        lock (_gate)
        {
            return new NotificationStatus(_queue.Count, _dropped, _delivered, _failed, _lastSuccess, _lastFailure, _lastError, _consecutiveFailures, _failingSince);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("NotificationDispatcher started.");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _signal.WaitAsync(stoppingToken).ConfigureAwait(false);
                Item? item;
                lock (_gate)
                {
                    item = _queue.First?.Value;
                    if (item != null) _queue.RemoveFirst();
                }

                if (item != null) await ProcessAsync(item, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // One bad item must never stop the loop.
                _logger.LogError(ex, "Notification processing failed unexpectedly.");
                RecordFailure(ex.Message);
            }
        }

        _logger.LogInformation("NotificationDispatcher stopped.");
    }

    private async Task ProcessAsync(Item item, CancellationToken stoppingToken)
    {
        string? digestKey = (item as DigestItem)?.Key;
        bool delivered = false;
        bool finished = false; // the digest was handled (sent, or no longer wanted) and must not be queued again today
        try
        {
            switch (item)
            {
                case AlertItem alert:
                    delivered = await WithBackstopAsync(ct => _alerts.DispatchAlertsAsync(alert.Job, alert.Run, alert.Reasons, ct), stoppingToken).ConfigureAwait(false);
                    if (delivered) await _jobs.UpdateAlertTimestampsAsync(alert.Job.Id, DateTime.UtcNow, null, stoppingToken).ConfigureAwait(false);
                    break;

                case DigestItem digest:
                    var job = await _jobs.GetByIdAsync(digest.JobId, stoppingToken).ConfigureAwait(false);
                    if (job is null || !job.IsEnabled) { finished = true; return; }

                    // Decide again on the state of now: an alert may have been sent since, or the digest may already have gone out.
                    var (should, _) = await _alerts.EvaluateDailyDigestConditionAsync(job, DateTime.UtcNow).ConfigureAwait(false);
                    if (!should)
                    {
                        lock (_gate) _digestKeys.Remove(digest.Key);
                        digestKey = null;
                        return;
                    }

                    var now = DateTime.UtcNow;
                    var recent = await _runs.GetRecentRunsAsync(100, job.Id, stoppingToken).ConfigureAwait(false);
                    var runs24h = recent.Where(r => r.StartTimeUtc >= now.AddHours(-24)).ToList();
                    var latest = recent.FirstOrDefault();
                    delivered = await WithBackstopAsync(ct => _alerts.DispatchDailyDigestAsync(job, latest, runs24h, ct), stoppingToken).ConfigureAwait(false);
                    if (delivered)
                    {
                        finished = true;
                        await _jobs.UpdateAlertTimestampsAsync(job.Id, null, DateTime.UtcNow, stoppingToken).ConfigureAwait(false);
                    }
                    break;
            }

            if (delivered) RecordSuccess();
            else RecordFailure(item is DigestItem ? "No channel delivered the daily digest." : "No channel delivered the alert.");
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Notification delivery failed ({Type}): {Message}", ex.GetType().Name, ex.Message);
            RecordFailure(ex.Message);
        }
        finally
        {
            if (digestKey != null && !finished)
            {
                // Not delivered: the scheduler may queue it again after a pause (not every 10 s against a dead server).
                lock (_gate) _digestKeys[digestKey] = Stopwatch.GetTimestamp() + (long)(DigestRetryDelay.TotalSeconds * Stopwatch.Frequency);
            }
        }
    }

    /// <summary>Runs a dispatch on its own task and gives up on it after the backstop, even when it ignores its token.</summary>
    private async Task<bool> WithBackstopAsync(Func<CancellationToken, Task<bool>> dispatch, CancellationToken stoppingToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        cts.CancelAfter(_backstop);
        var task = Task.Run(() => dispatch(cts.Token), CancellationToken.None);
        try
        {
            return await task.WaitAsync(_backstop, stoppingToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            cts.Cancel();
            _ = task.ContinueWith(t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
            throw new TimeoutException($"The notification dispatch did not finish within {_backstop.TotalSeconds:0.#} s and was abandoned.");
        }
    }

    private void RecordSuccess()
    {
        lock (_gate)
        {
            _delivered++;
            _lastSuccess = DateTime.UtcNow;
            _consecutiveFailures = 0;
            _failingSince = null;
        }
    }

    private void RecordFailure(string message)
    {
        lock (_gate)
        {
            _failed++;
            _lastFailure = DateTime.UtcNow;
            _lastError = message.Length > 300 ? message[..300] : message;
            _consecutiveFailures++;
            _failingSince ??= _lastFailure;
        }
    }
}
