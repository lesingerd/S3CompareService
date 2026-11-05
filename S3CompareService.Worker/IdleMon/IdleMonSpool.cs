using System.Globalization;
using System.Text;
using System.Text.Json;

namespace S3CompareService.Worker.IdleMon;

/// <summary>
/// The on-disk queue for batches that could not be sent. Files are <c>spool-{utc ticks}-{sequence}.jsonl</c>, one envelope per
/// line, replayed oldest first with the original message ids and timestamps. Files are written atomically (temp file, then
/// move) and only inside the configured folder. When the cap is exceeded, the oldest info, debug and trace events go first,
/// then the other non-alert messages; alerts and error events are never dropped (the cap is soft for them).
/// </summary>
public sealed class IdleMonSpool
{
    private readonly string _directory;
    private readonly long _maxBytes;
    private readonly TimeProvider _time;
    private readonly Action<string> _warn;
    private readonly object _gate = new();
    private long _sequence;
    private long _droppedMessages;

    /// <summary>Creates the spool. The folder is created on the first write.</summary>
    public IdleMonSpool(string directory, long maxBytes, TimeProvider time, Action<string>? warn = null)
    {
        _directory = directory;
        _maxBytes = maxBytes;
        _time = time;
        _warn = warn ?? (_ => { });
    }

    /// <summary>Gets the folder.</summary>
    public string Directory => _directory;

    /// <summary>Gets how many messages the cap removed.</summary>
    public long DroppedMessages => Interlocked.Read(ref _droppedMessages);

    /// <summary>Gets the number of files and their total size.</summary>
    public (int Files, long Bytes) Stats()
    {
        lock (_gate)
        {
            var files = List();
            return (files.Count, files.Sum(SizeOf));
        }
    }

    /// <summary>Writes envelopes (one per line) to a new spool file and enforces the cap. Returns the file path.</summary>
    public string Write(IReadOnlyList<byte[]> envelopes)
    {
        lock (_gate)
        {
            System.IO.Directory.CreateDirectory(_directory);
            var name = string.Create(CultureInfo.InvariantCulture, $"spool-{_time.GetUtcNow().UtcTicks:D19}-{++_sequence:D6}.jsonl");
            var path = Path.Combine(_directory, name);
            WriteLines(path, envelopes);
            Enforce();
            return path;
        }
    }

    /// <summary>Gets the oldest spool file and its envelopes, or null when the spool is empty.</summary>
    public (string Path, List<byte[]> Envelopes)? PeekOldest()
    {
        lock (_gate)
        {
            foreach (var path in List())
            {
                try
                {
                    var lines = File.ReadAllLines(path).Where(l => l.Length > 0).Select(Encoding.UTF8.GetBytes).ToList();
                    if (lines.Count == 0)
                    {
                        File.Delete(path);
                        continue;
                    }

                    return (path, lines);
                }
                catch (IOException ex)
                {
                    _warn($"Could not read spool file {Path.GetFileName(path)} ({ex.GetType().Name}).");
                    return null;
                }
            }

            return null;
        }
    }

    /// <summary>
    /// Gets the time of the oldest message still in the spool (the earliest message timestamp in the first envelope of the
    /// oldest file), or null when the spool is empty. Used for the heartbeat tag <c>backlogSeconds</c>.
    /// </summary>
    public DateTimeOffset? OldestMessageTime()
    {
        lock (_gate)
        {
            foreach (var path in List())
            {
                try
                {
                    var line = File.ReadLines(path).FirstOrDefault(l => l.Length > 0);
                    if (line is null) continue;
                    using var document = JsonDocument.Parse(line);
                    DateTimeOffset? oldest = null;
                    if (document.RootElement.TryGetProperty("messages", out var messages) && messages.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var message in messages.EnumerateArray())
                        {
                            if (message.TryGetProperty("timestamp", out var stamp)
                                && DateTimeOffset.TryParse(stamp.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var t)
                                && (oldest is null || t < oldest))
                            {
                                oldest = t;
                            }
                        }
                    }

                    return oldest ?? FromFileName(path);
                }
                catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
                {
                    return FromFileName(path);
                }
            }

            return null;
        }
    }

    /// <summary>Deletes a spool file after it was sent.</summary>
    public void Delete(string path)
    {
        lock (_gate)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException ex)
            {
                _warn($"Could not delete spool file {Path.GetFileName(path)} ({ex.GetType().Name}).");
            }
        }
    }

    /// <summary>Replaces a spool file by the envelopes that are still unsent.</summary>
    public void Rewrite(string path, IReadOnlyList<byte[]> remaining)
    {
        lock (_gate) WriteLines(path, remaining);
    }

    /// <summary>Appends one envelope line to the dry-run file (used instead of sending).</summary>
    public static void AppendLine(string path, byte[] envelope)
    {
        var directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory)) System.IO.Directory.CreateDirectory(directory);
        using var stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
        stream.Write(envelope);
        stream.WriteByte((byte)'\n');
    }

    // spool-{utc ticks}-{sequence}.jsonl: the ticks are when the file was written, a fallback for the message age.
    private static DateTimeOffset? FromFileName(string path)
    {
        var parts = Path.GetFileNameWithoutExtension(path).Split('-');
        return parts.Length >= 2 && long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var ticks) && ticks > 0 && ticks <= DateTime.MaxValue.Ticks
            ? new DateTimeOffset(new DateTime(ticks, DateTimeKind.Utc))
            : null;
    }

    private static long SizeOf(string path) => new FileInfo(path).Length;

    private static void WriteLines(string path, IReadOnlyList<byte[]> envelopes)
    {
        var temp = path + ".tmp";
        using (var stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
        {
            foreach (var envelope in envelopes)
            {
                stream.Write(envelope);
                stream.WriteByte((byte)'\n');
            }

            stream.Flush(flushToDisk: true);
        }

        File.Move(temp, path, overwrite: true);
    }

    /// <summary>Shrinks the spool under the cap in tiers; alerts and error events are never removed.</summary>
    private void Enforce()
    {
        if (List().Sum(SizeOf) <= _maxBytes) return;

        // Tier 1: info, debug and trace events. Tier 2: everything except alerts and error/critical events.
        foreach (var tier in new[] { 1, 2 })
        {
            foreach (var path in List())
            {
                if (List().Sum(SizeOf) <= _maxBytes) return;
                ShrinkFile(path, tier);
            }
        }

        if (List().Sum(SizeOf) > _maxBytes)
            _warn("IdleMon spool is over its cap but only alerts and error events remain; they are kept.");
    }

    private void ShrinkFile(string path, int tier)
    {
        try
        {
            var kept = new List<byte[]>();
            long removed = 0;
            foreach (var line in File.ReadAllLines(path).Where(l => l.Length > 0))
            {
                var (envelope, count) = Filter(Encoding.UTF8.GetBytes(line), tier);
                removed += count;
                if (envelope is not null) kept.Add(envelope);
            }

            if (removed == 0) return;
            Interlocked.Add(ref _droppedMessages, removed);
            _warn($"IdleMon spool is over its cap: removed {removed} old message(s) from {Path.GetFileName(path)}.");
            if (kept.Count == 0) File.Delete(path);
            else WriteLines(path, kept);
        }
        catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
        {
            _warn($"Could not shrink spool file {Path.GetFileName(path)} ({ex.GetType().Name}).");
        }
    }

    // Removes the droppable messages of an envelope. Returns the rebuilt envelope (null when nothing is left) and the number removed.
    private static (byte[]? Envelope, int Removed) Filter(byte[] envelope, int tier)
    {
        using var document = JsonDocument.Parse(envelope);
        var root = document.RootElement;
        if (!root.TryGetProperty("messages", out var messages) || messages.ValueKind != JsonValueKind.Array) return (envelope, 0);

        var keep = new List<byte[]>();
        var removed = 0;
        foreach (var message in messages.EnumerateArray())
        {
            if (Droppable(message, tier)) removed++;
            else keep.Add(Encoding.UTF8.GetBytes(message.GetRawText()));
        }

        if (removed == 0) return (envelope, 0);
        if (keep.Count == 0) return (null, removed);

        var source = root.GetProperty("source");
        var sentAt = root.TryGetProperty("sentAt", out var s) && DateTimeOffset.TryParse(s.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed) ? parsed : DateTimeOffset.UtcNow;
        return (IdleMonWire.BuildEnvelope(
            source.GetProperty("host").GetString() ?? "",
            source.TryGetProperty("appVersion", out var v) ? v.GetString() ?? "" : "",
            source.TryGetProperty("instance", out var i) ? i.GetString() ?? "default" : "default",
            sentAt,
            keep), removed);
    }

    private static bool Droppable(JsonElement message, int tier)
    {
        var kind = message.TryGetProperty("kind", out var k) ? k.GetString() : null;
        if (kind == "alert") return false;
        if (kind == "event")
        {
            var severity = message.TryGetProperty("severity", out var s) ? s.GetString() : null;
            var low = severity is "trace" or "debug" or "info";
            return tier == 1 ? low : severity is not ("error" or "critical");
        }

        return tier == 2;
    }

    private List<string> List() =>
        System.IO.Directory.Exists(_directory)
            ? System.IO.Directory.GetFiles(_directory, "spool-*.jsonl").Order(StringComparer.Ordinal).ToList()
            : [];
}
