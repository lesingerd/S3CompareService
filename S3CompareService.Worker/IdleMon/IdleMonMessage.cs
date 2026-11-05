using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace S3CompareService.Worker.IdleMon;

/// <summary>One column of a snapshot on the wire.</summary>
public sealed class IdleMonColumn
{
    /// <summary>Gets or sets the column name.</summary>
    public string Name { get; set; } = "";

    /// <summary>Gets or sets the type: string, number, bool or datetime.</summary>
    public string Type { get; set; } = "string";

    /// <summary>Gets or sets the optional unit.</summary>
    public string? Unit { get; set; }
}

/// <summary>
/// One message of contract v1. A single class for all five kinds; only the properties of the kind are set, the rest stay
/// null and are omitted. <c>Kind</c> is declared first so it is the first property on the wire. The builders create messages
/// without an id; the reporter assigns the id once, when the message is queued.
/// </summary>
public sealed class IdleMonMessage
{
    /// <summary>Gets or sets the kind: event, alert, metric, heartbeat or snapshot.</summary>
    public string Kind { get; set; } = "";

    /// <summary>Gets or sets the unique id, created once and kept across retries.</summary>
    public string Id { get; set; } = "";

    /// <summary>Gets or sets the UTC timestamp, yyyy-MM-ddTHH:mm:ss.fffZ.</summary>
    public string Timestamp { get; set; } = "";

    /// <summary>Gets or sets the severity (event, alert).</summary>
    public string? Severity { get; set; }

    /// <summary>Gets or sets the event category.</summary>
    public string? Category { get; set; }

    /// <summary>Gets or sets the event, alert or heartbeat text.</summary>
    public string? Message { get; set; }

    /// <summary>Gets or sets the alert key.</summary>
    public string? Key { get; set; }

    /// <summary>Gets or sets the alert status (firing or resolved) or the heartbeat status (ok, degraded or stopping).</summary>
    public string? Status { get; set; }

    /// <summary>Gets or sets the alert title.</summary>
    public string? Title { get; set; }

    /// <summary>Gets or sets the metric or snapshot name.</summary>
    public string? Name { get; set; }

    /// <summary>Gets or sets the metric value.</summary>
    public double? Value { get; set; }

    /// <summary>Gets or sets the metric unit.</summary>
    public string? Unit { get; set; }

    /// <summary>Gets or sets the metric type: counter (omitted for gauge).</summary>
    public string? Type { get; set; }

    /// <summary>Gets or sets the heartbeat interval.</summary>
    public int? IntervalSeconds { get; set; }

    /// <summary>Gets or sets the snapshot columns.</summary>
    public List<IdleMonColumn>? Columns { get; set; }

    /// <summary>Gets or sets the snapshot rows.</summary>
    public List<object?[]>? Rows { get; set; }

    /// <summary>Gets or sets the structured details (event, alert). Values are strings, longs, doubles, ints or booleans only.</summary>
    public Dictionary<string, object?>? Fields { get; set; }

    /// <summary>Gets or sets the tags.</summary>
    public Dictionary<string, string>? Tags { get; set; }

    // ---- factories (pure; the caller supplies the time) ----

    /// <summary>Creates an event.</summary>
    public static IdleMonMessage Event(DateTimeOffset at, string severity, string category, string message, Dictionary<string, object?>? fields = null, Dictionary<string, string>? tags = null) => new()
    {
        Kind = "event", Timestamp = IdleMonWire.Timestamp(at), Severity = severity, Category = category, Message = message, Fields = fields, Tags = tags,
    };

    /// <summary>Creates a firing alert.</summary>
    public static IdleMonMessage AlertFiring(DateTimeOffset at, string key, string severity, string title, string? message, Dictionary<string, object?>? fields = null, Dictionary<string, string>? tags = null) => new()
    {
        Kind = "alert", Timestamp = IdleMonWire.Timestamp(at), Key = IdleMonLimits.CapKey(key), Status = "firing", Severity = severity,
        Title = IdleMonLimits.CapTitle(title), Message = message, Fields = fields, Tags = tags,
    };

    /// <summary>Creates a resolved alert.</summary>
    public static IdleMonMessage AlertResolved(DateTimeOffset at, string key, Dictionary<string, string>? tags = null) => new()
    {
        Kind = "alert", Timestamp = IdleMonWire.Timestamp(at), Key = IdleMonLimits.CapKey(key), Status = "resolved", Tags = tags,
    };

    /// <summary>Creates a gauge metric.</summary>
    public static IdleMonMessage Metric(DateTimeOffset at, string name, double value, string unit, Dictionary<string, string>? tags = null) => new()
    {
        Kind = "metric", Timestamp = IdleMonWire.Timestamp(at), Name = name, Value = value, Unit = unit, Tags = tags,
    };

    /// <summary>Creates a heartbeat.</summary>
    public static IdleMonMessage Heartbeat(DateTimeOffset at, int intervalSeconds, string status, string? message, Dictionary<string, string>? tags = null) => new()
    {
        Kind = "heartbeat", Timestamp = IdleMonWire.Timestamp(at), IntervalSeconds = intervalSeconds, Status = status, Message = message, Tags = tags,
    };

    /// <summary>Creates a snapshot. Its timestamp is the time of its data.</summary>
    public static IdleMonMessage Snapshot(DateTimeOffset at, string name, List<IdleMonColumn> columns, List<object?[]> rows, Dictionary<string, string>? tags = null) => new()
    {
        Kind = "snapshot", Timestamp = IdleMonWire.Timestamp(at), Name = name, Columns = columns, Rows = rows, Tags = tags,
    };
}

/// <summary>Source-generated serialization for the wire model.</summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    WriteIndented = false)]
[JsonSerializable(typeof(IdleMonMessage))]
[JsonSerializable(typeof(Dictionary<string, object?>))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(bool))]
internal sealed partial class IdleMonJsonContext : JsonSerializerContext;

/// <summary>Helpers for the wire format.</summary>
public static class IdleMonWire
{
    /// <summary>The largest serialized message the contract accepts (64 KiB).</summary>
    public const int MaxMessageBytes = 64 * 1024;

    /// <summary>The contract's request limit is 1 MiB; batches are kept under this.</summary>
    public const int MaxBatchBytes = 900 * 1024;

    /// <summary>Formats a time as the contract wants: UTC, milliseconds, trailing Z.</summary>
    public static string Timestamp(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);

    /// <summary>Serializes a message to UTF-8 JSON.</summary>
    public static byte[] Serialize(IdleMonMessage message) =>
        JsonSerializer.SerializeToUtf8Bytes(message, IdleMonJsonContext.Default.IdleMonMessage);

    /// <summary>Gets the serialized size of a fields object (the contract limits it to 32 KiB).</summary>
    public static int FieldsSize(Dictionary<string, object?> fields) =>
        JsonSerializer.SerializeToUtf8Bytes(fields, IdleMonJsonContext.Default.DictionaryStringObject).Length;

    /// <summary>Reads a message back (tests, inspection).</summary>
    public static IdleMonMessage? Deserialize(ReadOnlySpan<byte> json) =>
        JsonSerializer.Deserialize(json, IdleMonJsonContext.Default.IdleMonMessage);

    /// <summary>Builds the envelope around already serialized messages.</summary>
    public static byte[] BuildEnvelope(string host, string version, string instance, DateTimeOffset sentAt, IReadOnlyList<byte[]> messages)
    {
        using var stream = new MemoryStream(messages.Sum(m => m.Length) + 512);
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteNumber("schemaVersion", 1);
            writer.WriteStartObject("source");
            writer.WriteString("app", IdleMonOptions.AppName);
            writer.WriteString("appVersion", version);
            writer.WriteString("instance", instance);
            writer.WriteString("host", host);
            writer.WriteEndObject();
            writer.WriteString("sentAt", Timestamp(sentAt));
            writer.WriteStartArray("messages");
            foreach (var message in messages) writer.WriteRawValue(message, skipInputValidation: true);
            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        return stream.ToArray();
    }

    /// <summary>Splits an envelope into two with half of the messages each (for HTTP 413). Null when it has fewer than two messages.</summary>
    public static (byte[] First, byte[] Second)? Split(byte[] envelope)
    {
        using var document = JsonDocument.Parse(envelope);
        var root = document.RootElement;
        if (!root.TryGetProperty("messages", out var messages) || messages.ValueKind != JsonValueKind.Array || messages.GetArrayLength() < 2) return null;

        var all = messages.EnumerateArray().Select(m => Encoding.UTF8.GetBytes(m.GetRawText())).ToList();
        var half = all.Count / 2;
        var source = root.GetProperty("source");
        var host = source.GetProperty("host").GetString() ?? "";
        var version = source.TryGetProperty("appVersion", out var v) ? v.GetString() ?? "" : "";
        var instance = source.TryGetProperty("instance", out var i) ? i.GetString() ?? "default" : "default";
        var sentAt = root.TryGetProperty("sentAt", out var s) && s.GetString() is { } text && DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed) ? parsed : DateTimeOffset.UtcNow;
        return (BuildEnvelope(host, version, instance, sentAt, all.Take(half).ToList()), BuildEnvelope(host, version, instance, sentAt, all.Skip(half).ToList()));
    }

    /// <summary>Counts the messages of an envelope, or 0 when it cannot be parsed.</summary>
    public static int CountMessages(byte[] envelope)
    {
        try
        {
            using var document = JsonDocument.Parse(envelope);
            return document.RootElement.TryGetProperty("messages", out var m) && m.ValueKind == JsonValueKind.Array ? m.GetArrayLength() : 0;
        }
        catch (JsonException)
        {
            return 0;
        }
    }

    /// <summary>Gets whether an envelope contains an alert message.</summary>
    public static bool ContainsAlert(byte[] envelope) =>
        Encoding.UTF8.GetString(envelope).Contains("\"kind\":\"alert\"", StringComparison.Ordinal);
}
