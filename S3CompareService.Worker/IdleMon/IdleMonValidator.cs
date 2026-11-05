using System.Globalization;
using System.Text.RegularExpressions;

namespace S3CompareService.Worker.IdleMon;

/// <summary>Checks a message against the contract v1 rules before it is queued. A message that fails is a bug in a builder: it is dropped and counted.</summary>
public static partial class IdleMonValidator
{
    private static readonly HashSet<string> EventSeverities = ["trace", "debug", "info", "warning", "error", "critical"];
    private static readonly HashSet<string> AlertSeverities = ["info", "warning", "error", "critical"];
    private static readonly HashSet<string> Units = ["count", "bytes", "seconds", "milliseconds", "percent", "bytes_per_second", "per_second", "none"];
    private static readonly HashSet<string> ColumnTypes = ["string", "number", "bool", "datetime"];

    [GeneratedRegex("^[a-z0-9_]+(\\.[a-z0-9_]+)*$")]
    private static partial Regex NamePattern();

    [GeneratedRegex("^[A-Za-z0-9_-]{1,64}$")]
    private static partial Regex IdPattern();

    [GeneratedRegex("^[A-Za-z0-9._-]{1,64}$")]
    private static partial Regex TagKeyPattern();

    [GeneratedRegex("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,7})?Z$")]
    private static partial Regex TimestampPattern();

    /// <summary>Returns the first rule the message breaks, or null when it is valid. <paramref name="requireId"/> is false before the reporter assigned the id.</summary>
    public static string? Validate(IdleMonMessage m, DateTimeOffset now, bool requireId = true)
    {
        if (requireId && !IdPattern().IsMatch(m.Id)) return "id";
        if (!TryParseTimestamp(m.Timestamp, out var at)) return "timestamp format";
        if (at > now.AddMinutes(5)) return "timestamp more than 5 minutes in the future";
        if (at < now.AddDays(-30)) return "timestamp more than 30 days in the past";

        if (m.Tags is { } tags)
        {
            if (tags.Count > 20) return "more than 20 tags";
            foreach (var (k, v) in tags)
            {
                if (!TagKeyPattern().IsMatch(k)) return "tag key";
                if (v is null || v.Length > 256) return "tag value";
            }
        }

        switch (m.Kind)
        {
            case "event":
                if (m.Severity is null || !EventSeverities.Contains(m.Severity)) return "event severity";
                if (m.Category is { Length: > 64 }) return "event category";
                if (string.IsNullOrWhiteSpace(m.Message) || m.Message.Length > 8192) return "event message";
                return CheckFields(m);
            case "alert":
                if (string.IsNullOrWhiteSpace(m.Key) || m.Key.Length > IdleMonLimits.MaxKeyOrTitle) return "alert key";
                if (m.Status is not ("firing" or "resolved")) return "alert status";
                if (m.Severity is not null && !AlertSeverities.Contains(m.Severity)) return "alert severity";
                if (m.Title is { Length: > IdleMonLimits.MaxKeyOrTitle }) return "alert title";
                if (m.Message is { Length: > 8192 }) return "alert message";
                if (m.Status == "firing" && (m.Severity is null || string.IsNullOrWhiteSpace(m.Title))) return "firing alert needs severity and title";
                return CheckFields(m);
            case "metric":
                if (m.Name is null || m.Name.Length > 128 || !NamePattern().IsMatch(m.Name)) return "metric name";
                if (m.Value is not { } value || !double.IsFinite(value)) return "metric value";
                if (m.Unit is not null && !Units.Contains(m.Unit)) return "metric unit";
                if (m.Type is not (null or "gauge" or "counter")) return "metric type";
                return null;
            case "heartbeat":
                if (m.IntervalSeconds is not { } interval || interval is < 10 or > 86400) return "heartbeat interval";
                if (m.Status is not (null or "ok" or "degraded" or "stopping")) return "heartbeat status";
                if (m.Message is { Length: > 1024 }) return "heartbeat message";
                return null;
            case "snapshot":
                return CheckSnapshot(m);
            default:
                return "kind";
        }
    }

    private static bool TryParseTimestamp(string? value, out DateTimeOffset at)
    {
        at = default;
        return value is not null
            && TimestampPattern().IsMatch(value)
            && DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out at);
    }

    private static string? CheckFields(IdleMonMessage m)
    {
        if (m.Fields is null) return null;
        foreach (var value in m.Fields.Values)
        {
            if (value is not (null or string or bool or int or long or double)) return "field value type";
            if (value is double d && !double.IsFinite(d)) return "field value";
        }

        return IdleMonWire.FieldsSize(m.Fields) > 32 * 1024 ? "fields larger than 32 KiB" : null;
    }

    private static string? CheckSnapshot(IdleMonMessage m)
    {
        if (m.Name is null || m.Name.Length > 128 || !NamePattern().IsMatch(m.Name)) return "snapshot name";
        if (m.Columns is not { Count: >= 1 and <= 32 } columns) return "snapshot columns";
        if (columns.Select(c => c.Name).Distinct(StringComparer.Ordinal).Count() != columns.Count) return "snapshot column names are not unique";
        foreach (var c in columns)
        {
            if (string.IsNullOrEmpty(c.Name) || c.Name.Length > 64) return "snapshot column name";
            if (!ColumnTypes.Contains(c.Type)) return "snapshot column type";
            if (c.Unit is { Length: > 32 }) return "snapshot column unit";
        }

        if (m.Rows is not { Count: <= 500 } rows) return "snapshot rows";
        foreach (var row in rows)
        {
            if (row.Length != columns.Count) return "snapshot row width";
            for (var i = 0; i < row.Length; i++)
            {
                var cell = row[i];
                if (cell is null) continue;
                var ok = columns[i].Type switch
                {
                    "string" => cell is string,
                    "number" => (cell is int or long) || (cell is double d && double.IsFinite(d)),
                    "bool" => cell is bool,
                    _ => cell is string s && TryParseTimestamp(s, out _),
                };
                if (!ok) return $"snapshot cell type in column {columns[i].Name}";
            }
        }

        return null;
    }
}
