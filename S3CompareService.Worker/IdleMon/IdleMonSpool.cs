using System.Globalization;
using System.Text;
using System.Text.Json;

namespace S3CompareService.Worker.IdleMon;

/// <summary>
/// The on-disk queue for batches that could not be sent. Files are <c>spool-{utc ticks}-{sequence}.jsonl</c>, one envelope per
/// line, replayed oldest first with the original message ids and timestamps. Files are written atomically (temp file, then
/// move) and only inside the configured folder.
/// <para>
/// The spool keeps an in-memory index of its files (size, oldest message, alert keys), built once at start and updated by every
/// change. So the cap is enforced in O(files) without touching the disk, and <see cref="Stats"/> and
/// <see cref="OldestMessageTime"/> (read by the status endpoint and the heartbeat) never do I/O or take the lock.
/// </para>
/// <para>
/// When the cap is exceeded: first the oldest info, debug and trace events go, then the other non-alert messages, then older
/// refreshes of alerts (only the latest firing/resolved message per alert key is kept; this also runs after every 100 alert
/// messages even under the cap, so weeks of outage do not pile up repeats). Error events are kept up to a hard ceiling of
/// twice the cap, above which the oldest ones go too. The latest state of every alert key is never dropped.
/// </para>
/// <para>
/// A file that cannot be read (access denied, locked) is skipped and retried every 30 seconds so it never blocks newer files or
/// fresh sends; after 24 hours of failing it is renamed to <c>.bad</c> and counted as dropped.
/// </para>
/// </summary>
public sealed class IdleMonSpool
{
    /// <summary>After this long of failing to read a file it is renamed to <c>.bad</c>.</summary>
    public static readonly TimeSpan UnreadableLimit = TimeSpan.FromHours(24);

    /// <summary>How often a file that could not be read is tried again.</summary>
    public static readonly TimeSpan UnreadableRetry = TimeSpan.FromSeconds(30);

    /// <summary>Alert messages written since the last coalescing that trigger another one.</summary>
    public const int CoalesceEveryAlerts = 100;

    private sealed class Entry
    {
        public required string Path { get; init; }
        public long Size { get; set; }
        public DateTimeOffset? Oldest { get; set; }
        public int Messages { get; set; } = -1; // -1: not read yet (a file from an earlier run)
        public bool HasDroppable { get; set; } = true; // unknown until read: assume yes
        public bool HasLowEvents { get; set; } = true; // info/debug/trace events (tier 1)
        public HashSet<string>? AlertKeys { get; set; } // null: not read yet
        public bool HasDuplicateAlerts { get; set; }
        public long FirstFailure { get; set; } // monotonic timestamp, 0 = none
        public long NextRetry { get; set; } // monotonic timestamp, 0 = now
        public bool Delivered { get; set; } // sent, the delete failed: only the delete is retried
    }

    private readonly record struct MessageInfo(string? Kind, string? Severity, string? Key, DateTimeOffset? Time);

    private sealed class ParsedMessage(byte[] raw, MessageInfo info)
    {
        public byte[] Raw { get; } = raw;
        public MessageInfo Info { get; } = info;
    }

    private sealed class ParsedEnvelope
    {
        public required byte[] Original { get; init; }
        public string Host { get; init; } = "";
        public string Version { get; init; } = "";
        public string Instance { get; init; } = "default";
        public DateTimeOffset SentAt { get; init; }
        public List<ParsedMessage> Messages { get; } = [];
    }

    private readonly string _directory;
    private readonly long _maxBytes;
    private readonly TimeProvider _time;
    private readonly Action<string> _warn;
    private readonly object _gate = new();
    private readonly SortedDictionary<string, Entry> _files = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _lastWarning = new();
    private bool _indexed;
    private long _totalBytes;
    private long _lastNameTicks;
    private long _sequence;
    private int _alertWritesSinceCoalesce;
    private long _droppedMessages;
    private long _badFiles;

    // Published after every change (under the lock), read without it.
    private long _publishedFiles;
    private long _publishedBytes;
    private long _publishedOldestTicks; // 0 = empty

    /// <summary>Creates the spool. The folder is created on the first write; existing files are indexed now.</summary>
    public IdleMonSpool(string directory, long maxBytes, TimeProvider time, Action<string>? warn = null)
    {
        _directory = directory;
        _maxBytes = maxBytes;
        _time = time;
        _warn = warn ?? (_ => { });
        lock (_gate) EnsureIndexed();
    }

    /// <summary>Gets the folder.</summary>
    public string Directory => _directory;

    /// <summary>Gets how many messages the cap removed (and an estimate for files given up as unreadable).</summary>
    public long DroppedMessages => Interlocked.Read(ref _droppedMessages);

    /// <summary>Gets how many unreadable files were renamed to <c>.bad</c>.</summary>
    public long BadFiles => Interlocked.Read(ref _badFiles);

    /// <summary>Gets the number of files and their total size. Served from memory.</summary>
    public (int Files, long Bytes) Stats() => ((int)Interlocked.Read(ref _publishedFiles), Interlocked.Read(ref _publishedBytes));

    /// <summary>
    /// Gets the time of the oldest message still in the spool (the earliest message timestamp of the oldest file; for a file
    /// left by an earlier run, the time it was written), or null when the spool is empty. Served from memory, never does I/O:
    /// the heartbeat reads it.
    /// </summary>
    public DateTimeOffset? OldestMessageTime()
    {
        var ticks = Interlocked.Read(ref _publishedOldestTicks);
        return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
    }

    /// <summary>Writes envelopes (one per line) to a new spool file and enforces the cap. Returns the file path.</summary>
    public string Write(IReadOnlyList<byte[]> envelopes)
    {
        lock (_gate)
        {
            EnsureIndexed();
            System.IO.Directory.CreateDirectory(_directory);

            // File names sort in write order. Wall-clock time can step backwards, so never go below the previous name.
            var ticks = Math.Max(_time.GetUtcNow().UtcTicks, _lastNameTicks + 1);
            _lastNameTicks = ticks;
            var name = string.Create(CultureInfo.InvariantCulture, $"spool-{ticks:D19}-{++_sequence:D6}.jsonl");
            var path = Path.Combine(_directory, name);
            var size = WriteLines(path, envelopes);

            var entry = new Entry { Path = path };
            _files[path] = entry;
            _totalBytes += size;
            entry.Size = size;
            ApplyProfile(entry, envelopes.Select(Parse).ToList());
            _alertWritesSinceCoalesce += entry.AlertKeys?.Count ?? 0;
            SupersedeOlderAlerts(entry);
            Publish();

            Enforce();
            Publish();
            return path;
        }
    }

    /// <summary>
    /// Gets the oldest readable spool file and its envelopes, or null when there is none. A file that cannot be read is skipped
    /// (see the class remarks), so this never blocks the sender; it never throws for I/O problems.
    /// </summary>
    public (string Path, List<byte[]> Envelopes)? PeekOldest()
    {
        lock (_gate)
        {
            EnsureIndexed();
            long now = _time.GetTimestamp();
            foreach (var entry in _files.Values.ToList())
            {
                if (entry.NextRetry != 0 && _time.GetElapsedTime(entry.NextRetry, now) < TimeSpan.Zero) continue; // not due yet

                if (entry.Delivered)
                {
                    TryDelete(entry);
                    continue;
                }

                try
                {
                    var lines = File.ReadAllLines(entry.Path).Where(l => l.Length > 0).Select(Encoding.UTF8.GetBytes).ToList();
                    if (lines.Count == 0)
                    {
                        File.Delete(entry.Path);
                        RemoveEntry(entry);
                        continue;
                    }

                    entry.FirstFailure = 0;
                    entry.NextRetry = 0;
                    ApplyProfile(entry, lines.Select(Parse).ToList()); // refines a file from an earlier run
                    Publish();
                    return (entry.Path, lines);
                }
                catch (FileNotFoundException)
                {
                    RemoveEntry(entry); // deleted behind our back
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    HandleUnreadable(entry, now, ex);
                }
            }

            Publish();
            return null;
        }
    }

    /// <summary>Deletes a spool file after it was sent. If the delete fails the file is remembered as sent and only deleted later.</summary>
    public void Delete(string path)
    {
        lock (_gate)
        {
            if (!_files.TryGetValue(path, out var entry))
            {
                try { File.Delete(path); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { Warn("delete-" + path, $"Could not delete spool file {Path.GetFileName(path)} ({ex.GetType().Name})."); }
                return;
            }

            if (!TryDelete(entry))
            {
                entry.Delivered = true; // never send it again
                entry.NextRetry = _time.GetTimestamp() + _time.TimestampFrequency * 30;
            }

            Publish();
        }
    }

    /// <summary>Replaces a spool file by the envelopes that are still unsent.</summary>
    public void Rewrite(string path, IReadOnlyList<byte[]> remaining)
    {
        lock (_gate)
        {
            try
            {
                var size = WriteLines(path, remaining);
                if (_files.TryGetValue(path, out var entry))
                {
                    _totalBytes += size - entry.Size;
                    entry.Size = size;
                    ApplyProfile(entry, remaining.Select(Parse).ToList());
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // The old file stays as it was: already-sent messages in it are sent again and removed by id at the hub.
                Warn("rewrite-" + path, $"Could not rewrite spool file {Path.GetFileName(path)} ({ex.GetType().Name}).");
            }

            Publish();
        }
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

    // ---- index ----

    private void EnsureIndexed()
    {
        if (_indexed) return;
        try
        {
            if (System.IO.Directory.Exists(_directory))
            {
                foreach (var path in System.IO.Directory.GetFiles(_directory, "spool-*.jsonl"))
                {
                    _files[path] = new Entry { Path = path, Size = new FileInfo(path).Length, Oldest = FromFileName(path) };
                    var ticks = FromFileName(path)?.UtcTicks ?? 0;
                    if (ticks > _lastNameTicks) _lastNameTicks = ticks;
                }

                // A temp file left by a crash during a write is useless (the move never happened).
                foreach (var temp in System.IO.Directory.GetFiles(_directory, "spool-*.jsonl.tmp"))
                {
                    try { File.Delete(temp); }
                    catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { /* next start */ }
                }
            }

            _totalBytes = _files.Values.Sum(e => e.Size);
            _indexed = true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _files.Clear();
            _totalBytes = 0;
            Warn("index", $"Could not read the spool folder ({ex.GetType().Name}); will try again.");
        }

        Publish();
    }

    private void Publish()
    {
        Interlocked.Exchange(ref _publishedFiles, _files.Count);
        Interlocked.Exchange(ref _publishedBytes, _totalBytes);
        DateTimeOffset? oldest = null;
        foreach (var entry in _files.Values)
        {
            if (entry.Delivered) continue;
            oldest = entry.Oldest ?? FromFileName(entry.Path);
            break;
        }

        Interlocked.Exchange(ref _publishedOldestTicks, oldest?.UtcTicks ?? 0);
    }

    private void RemoveEntry(Entry entry)
    {
        if (_files.Remove(entry.Path)) _totalBytes -= entry.Size;
    }

    private bool TryDelete(Entry entry)
    {
        try
        {
            File.Delete(entry.Path);
            RemoveEntry(entry);
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Warn("delete-" + entry.Path, $"Could not delete spool file {Path.GetFileName(entry.Path)} ({ex.GetType().Name}).");
            return false;
        }
    }

    private void HandleUnreadable(Entry entry, long now, Exception ex)
    {
        if (entry.FirstFailure == 0)
        {
            entry.FirstFailure = now;
            _warn($"Could not read spool file {Path.GetFileName(entry.Path)} ({ex.GetType().Name}); it is skipped and retried.");
        }

        if (_time.GetElapsedTime(entry.FirstFailure, now) >= UnreadableLimit)
        {
            var lost = Math.Max(1, entry.Messages);
            try
            {
                File.Move(entry.Path, entry.Path + ".bad", overwrite: true);
                RemoveEntry(entry);
                Interlocked.Add(ref _droppedMessages, lost);
                Interlocked.Increment(ref _badFiles);
                _warn($"Spool file {Path.GetFileName(entry.Path)} could not be read for 24 hours; renamed to .bad ({lost} message(s) counted as dropped).");
                return;
            }
            catch (Exception moveEx) when (moveEx is IOException or UnauthorizedAccessException)
            {
                Warn("bad-" + entry.Path, $"Could not rename unreadable spool file {Path.GetFileName(entry.Path)} ({moveEx.GetType().Name}).");
            }
        }

        entry.NextRetry = now + (long)(_time.TimestampFrequency * UnreadableRetry.TotalSeconds);
    }

    private void Warn(string key, string message)
    {
        long now = _time.GetTimestamp();
        if (_lastWarning.TryGetValue(key, out var last) && _time.GetElapsedTime(last, now) < TimeSpan.FromMinutes(1)) return;
        if (_lastWarning.Count > 256) _lastWarning.Clear();
        _lastWarning[key] = now;
        _warn(message);
    }

    // ---- files ----

    // spool-{utc ticks}-{sequence}.jsonl: the ticks are when the file was written, a fallback for the message age.
    private static DateTimeOffset? FromFileName(string path)
    {
        var parts = Path.GetFileNameWithoutExtension(path).Split('-');
        return parts.Length >= 2 && long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var ticks) && ticks > 0 && ticks <= DateTime.MaxValue.Ticks
            ? new DateTimeOffset(new DateTime(ticks, DateTimeKind.Utc))
            : null;
    }

    private static long WriteLines(string path, IReadOnlyList<byte[]> envelopes)
    {
        var temp = path + ".tmp";
        long size = 0;
        using (var stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
        {
            foreach (var envelope in envelopes)
            {
                stream.Write(envelope);
                stream.WriteByte((byte)'\n');
                size += envelope.Length + 1;
            }

            stream.Flush(flushToDisk: true);
        }

        File.Move(temp, path, overwrite: true);
        return size;
    }

    // ---- parsing and profiles ----

    private static ParsedEnvelope Parse(byte[] envelope)
    {
        try
        {
            using var document = JsonDocument.Parse(envelope);
            var root = document.RootElement;
            if (!root.TryGetProperty("messages", out var messages) || messages.ValueKind != JsonValueKind.Array || !root.TryGetProperty("source", out var source))
                return new ParsedEnvelope { Original = envelope };

            var parsed = new ParsedEnvelope
            {
                Original = envelope,
                Host = source.TryGetProperty("host", out var h) ? h.GetString() ?? "" : "",
                Version = source.TryGetProperty("appVersion", out var v) ? v.GetString() ?? "" : "",
                Instance = source.TryGetProperty("instance", out var i) ? i.GetString() ?? "default" : "default",
                SentAt = ParseTime(root, "sentAt") ?? DateTimeOffset.UtcNow
            };
            foreach (var message in messages.EnumerateArray())
            {
                var info = new MessageInfo(
                    message.TryGetProperty("kind", out var k) ? k.GetString() : null,
                    message.TryGetProperty("severity", out var s) ? s.GetString() : null,
                    message.TryGetProperty("key", out var key) ? key.GetString() : null,
                    ParseTime(message, "timestamp"));
                parsed.Messages.Add(new ParsedMessage(Encoding.UTF8.GetBytes(message.GetRawText()), info));
            }

            return parsed;
        }
        catch (JsonException)
        {
            return new ParsedEnvelope { Original = envelope };
        }
    }

    private static DateTimeOffset? ParseTime(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
        && DateTimeOffset.TryParse(value.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var t)
            ? t
            : null;

    private static bool IsAlert(MessageInfo m) => m.Kind == "alert";

    private static bool IsLowEvent(MessageInfo m) => m.Kind == "event" && m.Severity is "trace" or "debug" or "info";

    private static bool IsErrorEvent(MessageInfo m) => m.Kind == "event" && m.Severity is "error" or "critical";

    // What the cap may remove: tier 1 info events; tier 2 everything but alerts and error events; tier 3 (hard ceiling) everything but alerts.
    private static bool Droppable(MessageInfo m, int tier) => tier switch
    {
        1 => IsLowEvent(m),
        2 => !IsAlert(m) && !IsErrorEvent(m),
        _ => !IsAlert(m)
    };

    private void ApplyProfile(Entry entry, IReadOnlyList<ParsedEnvelope> envelopes)
    {
        int count = 0;
        DateTimeOffset? oldest = null;
        bool droppable = false;
        bool low = false;
        var keys = new HashSet<string>(StringComparer.Ordinal);
        bool duplicates = false;
        foreach (var message in envelopes.SelectMany(e => e.Messages))
        {
            count++;
            if (message.Info.Time is { } t && (oldest is null || t < oldest)) oldest = t;
            if (Droppable(message.Info, 2)) droppable = true;
            if (IsLowEvent(message.Info)) low = true;
            if (IsAlert(message.Info) && message.Info.Key is { } key && !keys.Add(key)) duplicates = true;
        }

        entry.Messages = count;
        entry.Oldest = oldest ?? entry.Oldest ?? FromFileName(entry.Path);
        entry.HasDroppable = droppable;
        entry.HasLowEvents = low;
        entry.AlertKeys = keys;
        entry.HasDuplicateAlerts = duplicates;
    }

    // ---- cap ----

    /// <summary>Shrinks the spool under the cap in tiers; the latest state of every alert is never removed.</summary>
    private void Enforce()
    {
        if (_totalBytes <= _maxBytes)
        {
            // Under the cap: still keep repeated alert refreshes from piling up.
            if (_alertWritesSinceCoalesce >= CoalesceEveryAlerts) CoalesceAlerts();
            return;
        }

        foreach (var tier in new[] { 1, 2 })
        {
            foreach (var entry in _files.Values.ToList())
            {
                if (_totalBytes <= _maxBytes) return;
                if (!entry.Delivered && (tier == 1 ? entry.HasLowEvents : entry.HasDroppable)) Shrink(entry, m => Droppable(m, tier));
            }
        }

        if (_totalBytes > _maxBytes) CoalesceAlerts();

        // Hard ceiling: even error events go (oldest first). Alerts still stay, one per key.
        long ceiling = _maxBytes > long.MaxValue / 2 ? long.MaxValue : _maxBytes * 2;
        if (_totalBytes > ceiling)
        {
            foreach (var entry in _files.Values.ToList())
            {
                if (_totalBytes <= ceiling) break;
                if (!entry.Delivered) Shrink(entry, m => Droppable(m, 3));
            }
        }

        if (_totalBytes > _maxBytes)
            Warn("over-cap", "IdleMon spool is over its cap but only alerts (the latest per key) and error events remain; they are kept.");
    }

    /// <summary>Keeps only the newest firing/resolved message per alert key (across files and inside a file).</summary>
    private void CoalesceAlerts()
    {
        _alertWritesSinceCoalesce = 0;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in _files.Values.Reverse().ToList()) // newest first
        {
            if (entry.Delivered) continue;
            if (entry.AlertKeys is null || entry.Messages < 0)
            {
                if (!TryLoadProfile(entry)) continue;
            }

            if (entry.AlertKeys!.Count == 0) continue;
            if (entry.HasDuplicateAlerts || entry.AlertKeys.Overlaps(seen))
            {
                DropSupersededAlerts(entry, seen.Contains);
            }

            if (entry.AlertKeys is not null) seen.UnionWith(entry.AlertKeys);
        }
    }

    // Inside a file the last message of a key wins; a message is also dropped when a newer message of its key exists elsewhere.
    private void DropSupersededAlerts(Entry entry, Func<string, bool> newerExists) =>
        RewriteFiltered(entry, parsed =>
        {
            var drop = new HashSet<ParsedMessage>();
            var later = new HashSet<string>(StringComparer.Ordinal);
            foreach (var message in parsed.SelectMany(e => e.Messages).Reverse())
            {
                if (!IsAlert(message.Info) || message.Info.Key is not { } key) continue;
                if (newerExists(key) || !later.Add(key)) drop.Add(message);
            }

            return drop;
        });

    // A new file holds the latest state of its alert keys: older messages of those keys (in earlier files) are now redundant.
    private void SupersedeOlderAlerts(Entry fresh)
    {
        if (fresh.AlertKeys is not { Count: > 0 } keys) return;
        if (fresh.HasDuplicateAlerts) DropSupersededAlerts(fresh, _ => false);
        foreach (var older in _files.Values.ToList())
        {
            if (ReferenceEquals(older, fresh) || older.Delivered || older.AlertKeys is null) continue; // unknown profiles: see CoalesceAlerts
            if (older.AlertKeys.Overlaps(fresh.AlertKeys!)) DropSupersededAlerts(older, fresh.AlertKeys!.Contains);
        }
    }

    private bool TryLoadProfile(Entry entry)
    {
        try
        {
            var lines = File.ReadAllLines(entry.Path).Where(l => l.Length > 0).Select(Encoding.UTF8.GetBytes);
            ApplyProfile(entry, lines.Select(Parse).ToList());
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Warn("profile-" + entry.Path, $"Could not read spool file {Path.GetFileName(entry.Path)} ({ex.GetType().Name}).");
            return false;
        }
    }

    private void Shrink(Entry entry, Func<MessageInfo, bool> drop) =>
        RewriteFiltered(entry, parsed => parsed.SelectMany(e => e.Messages).Where(m => drop(m.Info)).ToHashSet());

    // Removes the chosen messages from a file (rewrites it, or deletes it when nothing is left). Never throws for I/O problems.
    private void RewriteFiltered(Entry entry, Func<List<ParsedEnvelope>, HashSet<ParsedMessage>> choose)
    {
        try
        {
            var parsed = File.ReadAllLines(entry.Path).Where(l => l.Length > 0).Select(l => Parse(Encoding.UTF8.GetBytes(l))).ToList();
            var toDrop = choose(parsed);
            if (toDrop.Count == 0)
            {
                ApplyProfile(entry, parsed);
                return;
            }

            var kept = new List<byte[]>();
            var keptEnvelopes = new List<ParsedEnvelope>();
            foreach (var envelope in parsed)
            {
                if (envelope.Messages.Count == 0)
                {
                    kept.Add(envelope.Original); // not understood: never touched
                    keptEnvelopes.Add(envelope);
                    continue;
                }

                var remaining = envelope.Messages.Where(m => !toDrop.Contains(m)).ToList();
                if (remaining.Count == 0) continue;
                if (remaining.Count == envelope.Messages.Count)
                {
                    kept.Add(envelope.Original);
                    keptEnvelopes.Add(envelope);
                    continue;
                }

                var rebuilt = IdleMonWire.BuildEnvelope(envelope.Host, envelope.Version, envelope.Instance, envelope.SentAt, remaining.Select(m => m.Raw).ToList());
                kept.Add(rebuilt);
                var copy = new ParsedEnvelope { Original = rebuilt, Host = envelope.Host, Version = envelope.Version, Instance = envelope.Instance, SentAt = envelope.SentAt };
                copy.Messages.AddRange(remaining);
                keptEnvelopes.Add(copy);
            }

            Interlocked.Add(ref _droppedMessages, toDrop.Count(m => !IsAlertRefreshOnly(m)));
            if (kept.Count == 0)
            {
                File.Delete(entry.Path);
                RemoveEntry(entry);
            }
            else
            {
                var size = WriteLines(entry.Path, kept);
                _totalBytes += size - entry.Size;
                entry.Size = size;
                ApplyProfile(entry, keptEnvelopes);
            }

            Warn("shrunk", $"IdleMon spool is over its cap or holds repeated alerts: removed {toDrop.Count} old message(s) from {Path.GetFileName(entry.Path)}.");
        }
        catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
        {
            Warn("shrink-" + entry.Path, $"Could not shrink spool file {Path.GetFileName(entry.Path)} ({ex.GetType().Name}).");
        }
    }

    // Older refreshes of an alert are replaced by a newer one, so they are not "lost" in the dropped-messages count.
    private static bool IsAlertRefreshOnly(ParsedMessage message) => IsAlert(message.Info);
}
