namespace S3CompareService.Worker.IdleMon;

/// <summary>How much a message may be sacrificed when the queue is full.</summary>
public enum MessagePriority
{
    /// <summary>Events of severity info and below: dropped first.</summary>
    Low = 0,

    /// <summary>Metrics, snapshots, heartbeats and warning events.</summary>
    Normal = 1,

    /// <summary>Alerts and error events: never dropped.</summary>
    High = 2,
}

/// <summary>One serialized message waiting to be sent.</summary>
public sealed record PendingMessage(byte[] Json, MessagePriority Priority);

/// <summary>
/// The bounded in-memory queue between the reporter and the sender. When it is full, the oldest low-priority message is
/// dropped first, then the oldest normal one; an alert is never dropped (a full queue of alerts grows past the bound).
/// </summary>
public sealed class IdleMonQueue(int capacity = 5000, int signalAt = 200)
{
    private readonly object _gate = new();
    private readonly LinkedList<PendingMessage> _items = new();
    private readonly SemaphoreSlim _signal = new(0, 1);
    private long _dropped;

    /// <summary>Gets the number of queued messages.</summary>
    public int Count
    {
        get
        {
            lock (_gate) return _items.Count;
        }
    }

    /// <summary>Gets how many messages were dropped because the queue was full.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Adds a message. Returns false when this message itself was dropped.</summary>
    public bool Add(PendingMessage message)
    {
        int count;
        lock (_gate)
        {
            if (_items.Count >= capacity)
            {
                if (message.Priority != MessagePriority.High && !TryDropOldestBelow(message.Priority))
                {
                    Interlocked.Increment(ref _dropped);
                    return false;
                }

                if (message.Priority == MessagePriority.High) TryDropOldestBelow(MessagePriority.High);
            }

            _items.AddLast(message);
            count = _items.Count;
        }

        if (count >= signalAt) Wake();
        return true;
    }

    /// <summary>Takes up to <paramref name="maxMessages"/> messages in arrival order, keeping the total under <paramref name="maxBytes"/>.</summary>
    public List<PendingMessage> Take(int maxMessages, int maxBytes)
    {
        var batch = new List<PendingMessage>();
        var bytes = 0;
        lock (_gate)
        {
            while (_items.First is { } first && batch.Count < maxMessages)
            {
                if (batch.Count > 0 && bytes + first.Value.Json.Length + 1 > maxBytes) break;
                bytes += first.Value.Json.Length + 1;
                batch.Add(first.Value);
                _items.RemoveFirst();
            }
        }

        return batch;
    }

    /// <summary>Waits until a wake-up (200 messages queued, or shutdown), or the timeout passes.</summary>
    public Task<bool> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken) => _signal.WaitAsync(timeout, cancellationToken);

    /// <summary>Wakes a waiting sender.</summary>
    public void Wake()
    {
        try
        {
            _signal.Release();
        }
        catch (SemaphoreFullException)
        {
            // Already signalled.
        }
    }

    private bool TryDropOldestBelow(MessagePriority incoming)
    {
        foreach (var level in new[] { MessagePriority.Low, MessagePriority.Normal })
        {
            if (level >= incoming) break;
            for (var node = _items.First; node is not null; node = node.Next)
            {
                if (node.Value.Priority == level)
                {
                    _items.Remove(node);
                    Interlocked.Increment(ref _dropped);
                    return true;
                }
            }
        }

        return false;
    }
}
