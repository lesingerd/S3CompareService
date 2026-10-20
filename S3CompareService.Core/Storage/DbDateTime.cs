using System.Globalization;

namespace S3CompareService.Core.Storage;

/// <summary>
/// Single place for converting DateTime values to/from the ISO-8601 TEXT columns used in SQLite.
/// All values are persisted as UTC ("O" format) and always read back with Kind=Utc.
/// </summary>
public static class DbDateTime
{
    /// <summary>Formats a value for storage. Local values are converted; Unspecified values are assumed to already be UTC.</summary>
    public static string ToDb(DateTime value) => ToUtc(value).ToString("O", CultureInfo.InvariantCulture);

    public static string? ToDb(DateTime? value) => value.HasValue ? ToDb(value.Value) : null;

    /// <summary>Parses a stored value; the result always has Kind=Utc. (RoundtripKind cannot be combined with AdjustToUniversal; AssumeUniversal covers offset-less strings.)</summary>
    public static DateTime Parse(string value)
        => DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

    public static DateTime Parse(object? value) => Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);

    public static DateTime? ParseNullable(object? value)
        => value == null || value is DBNull ? null : Parse(value);

    /// <summary>Normalizes an arbitrary DateTime (e.g. from JSON) to Kind=Utc.</summary>
    public static DateTime ToUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Utc => value,
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => DateTime.SpecifyKind(value, DateTimeKind.Utc)
    };

    public static DateTime? ToUtc(DateTime? value) => value.HasValue ? ToUtc(value.Value) : null;
}
