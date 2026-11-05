using Microsoft.Extensions.Logging;

namespace S3CompareService.Worker.IdleMon;

/// <summary>What the status endpoint shows about the reporter. All members are thread-safe. Always registered, also when reporting is off.</summary>
public sealed class IdleMonStatus
{
    private readonly object _gate = new();
    private long _accepted;
    private long _rejected;
    private long _spooled;
    private long _dropped;
    private DateTimeOffset? _lastSendAt;
    private string? _lastError;

    /// <summary>Gets or sets whether reporting is on.</summary>
    public bool Enabled { get; set; }

    /// <summary>Gets or sets Disabled, Http or DryRun.</summary>
    public string Mode { get; set; } = "Disabled";

    /// <summary>Gets or sets why reporting was switched off although it was enabled in configuration.</summary>
    public string? ConfigProblem { get; set; }

    /// <summary>Gets or sets the queue (null when disabled).</summary>
    public IdleMonQueue? Queue { get; set; }

    /// <summary>Gets or sets the spool (null when disabled or in dry-run mode).</summary>
    public IdleMonSpool? Spool { get; set; }

    /// <summary>Gets the number of messages IdleMon accepted (or the dry-run file received).</summary>
    public long Accepted => Interlocked.Read(ref _accepted);

    /// <summary>Gets the number of messages IdleMon rejected as invalid.</summary>
    public long Rejected => Interlocked.Read(ref _rejected);

    /// <summary>Gets the number of messages written to the spool.</summary>
    public long Spooled => Interlocked.Read(ref _spooled);

    /// <summary>Gets the number of messages dropped (queue overflow, invalid, spool failures, bad requests).</summary>
    public long Dropped => Interlocked.Read(ref _dropped) + (Queue?.Dropped ?? 0) + (Spool?.DroppedMessages ?? 0);

    /// <summary>Gets the time of the last send attempt that reached a verdict (accepted, rejected or failed).</summary>
    public DateTimeOffset? LastSendAt { get { lock (_gate) return _lastSendAt; } }

    /// <summary>Gets the last problem, or null when the last send worked.</summary>
    public string? LastError { get { lock (_gate) return _lastError; } }

    /// <summary>Adds accepted messages.</summary>
    public void AddAccepted(long count) => Interlocked.Add(ref _accepted, count);

    /// <summary>Adds rejected messages.</summary>
    public void AddRejected(long count) => Interlocked.Add(ref _rejected, count);

    /// <summary>Adds spooled messages.</summary>
    public void AddSpooled(long count) => Interlocked.Add(ref _spooled, count);

    /// <summary>Adds dropped messages.</summary>
    public void AddDropped(long count) => Interlocked.Add(ref _dropped, count);

    /// <summary>Records the outcome of a send attempt. A null error means success.</summary>
    public void SetLastSend(DateTimeOffset at, string? error)
    {
        lock (_gate)
        {
            _lastSendAt = at;
            _lastError = error;
        }
    }

    /// <summary>The JSON shape of the status endpoint.</summary>
    public object ToApiModel() => new
    {
        Enabled,
        Mode,
        ConfigProblem,
        LastSendAt = LastSendAt?.UtcDateTime,
        LastError,
        QueueLength = Queue?.Count ?? 0,
        SpoolFiles = Spool?.Stats().Files ?? 0,
        SpoolBytes = Spool?.Stats().Bytes ?? 0,
        Accepted,
        Rejected,
        Spooled,
        Dropped,
    };
}

/// <summary>The only thing the rest of the service sees of the sending machinery.</summary>
public interface IIdleMonReporter
{
    /// <summary>Gets whether messages are really sent.</summary>
    bool IsEnabled { get; }

    /// <summary>Validates, stamps with an id and queues a message. Returns immediately and never throws.</summary>
    void Submit(IdleMonMessage message);
}

/// <summary>Used when reporting is off: does nothing at all.</summary>
public sealed class NullIdleMonReporter : IIdleMonReporter
{
    /// <inheritdoc />
    public bool IsEnabled => false;

    /// <inheritdoc />
    public void Submit(IdleMonMessage message)
    {
    }
}

/// <summary>
/// The real reporter: validates a message against the contract, gives it its id (once; the id is kept across retries and the
/// spool), serializes it and puts it into the bounded queue. Sending is the job of <see cref="IdleMonSender"/>.
/// </summary>
public sealed class IdleMonReporter(IdleMonQueue queue, IdleMonStatus status, TimeProvider time, ILogger<IdleMonReporter> logger) : IIdleMonReporter
{
    private int _invalidLogged;

    /// <inheritdoc />
    public bool IsEnabled => true;

    /// <inheritdoc />
    public void Submit(IdleMonMessage message)
    {
        try
        {
            if (string.IsNullOrEmpty(message.Id)) message.Id = Guid.NewGuid().ToString("N");
            var problem = IdleMonValidator.Validate(message, time.GetUtcNow());
            if (problem is not null)
            {
                Reject($"a {message.Kind} message was invalid ({problem}) and was dropped");
                return;
            }

            var json = IdleMonWire.Serialize(message);
            if (json.Length > IdleMonWire.MaxMessageBytes)
            {
                Reject($"a {message.Kind} message of {json.Length} bytes is over the 64 KiB limit and was dropped");
                return;
            }

            queue.Add(new PendingMessage(json, PriorityOf(message)));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Never throw into the caller.
            Reject("a message could not be queued (" + ex.GetType().Name + ")");
        }
    }

    /// <summary>Gets the drop priority of a message.</summary>
    public static MessagePriority PriorityOf(IdleMonMessage message) => message.Kind switch
    {
        "alert" => MessagePriority.High,
        "event" => message.Severity switch
        {
            "error" or "critical" => MessagePriority.High,
            "warning" => MessagePriority.Normal,
            _ => MessagePriority.Low,
        },
        _ => MessagePriority.Normal,
    };

    private void Reject(string text)
    {
        status.AddDropped(1);
        if (Interlocked.Exchange(ref _invalidLogged, 1) == 0)
            logger.LogWarning("IdleMon reporter: {Text}. Further occurrences are only counted.", text);
    }
}
