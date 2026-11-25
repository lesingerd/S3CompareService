using System.Text.Json;

namespace S3CompareService.Worker;

/// <summary>
/// The optional local override file <c>appsettings.Local.json</c>. It lives in the data directory, not in the install folder,
/// so a service update (which overwrites the install folder) never touches it. It is read once at start-up.
/// </summary>
public static class LocalSettings
{
    /// <summary>The file name inside the data directory.</summary>
    public const string FileName = "appsettings.Local.json";

    /// <summary>Gets the full path of the override file for a data directory.</summary>
    public static string PathFor(string dataDirectory) =>
        Path.Combine(Path.GetFullPath(dataDirectory.Trim().Trim('"')), FileName);

    /// <summary>What happened to the override file at start-up (for the audit log).</summary>
    public sealed record LoadResult(string Path, bool Exists, bool Loaded, string? Error, IReadOnlyList<string> Settings, long Bytes, DateTime? ModifiedUtc);

    /// <summary>
    /// Adds the override file (if it exists and is valid JSON) after appsettings.json, then puts environment variables and the
    /// command line back on top, so that command-line arguments still win. Changes need a service restart (<c>reloadOnChange</c>
    /// is off). A file that cannot be read or parsed is skipped (the service starts with the other settings) instead of stopping
    /// the service; the returned result says why, so it can be written to the audit log.
    /// </summary>
    public static LoadResult Apply(IConfigurationBuilder configuration, string path, string[] args)
    {
        var result = Inspect(path);
        if (result.Loaded) configuration.AddJsonFile(path, optional: true, reloadOnChange: false);
        configuration.AddEnvironmentVariables();
        configuration.AddCommandLine(args);
        return result;
    }

    /// <summary>Reads and parses the file the way the configuration system does (comments and trailing commas allowed).</summary>
    public static LoadResult Inspect(string path)
    {
        if (!File.Exists(path)) return new LoadResult(path, false, false, null, Array.Empty<string>(), 0, null);
        try
        {
            var info = new FileInfo(path);
            string text = File.ReadAllText(path);
            using var doc = JsonDocument.Parse(text, new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true });
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return new LoadResult(path, true, false, "the top level must be a JSON object ({ ... })", Array.Empty<string>(), info.Length, info.LastWriteTimeUtc);
            var settings = new List<string>();
            Flatten(doc.RootElement, string.Empty, settings);
            return new LoadResult(path, true, true, null, settings, info.Length, info.LastWriteTimeUtc);
        }
        catch (Exception ex)
        {
            return new LoadResult(path, true, false, $"{ex.GetType().Name}: {ex.Message}", Array.Empty<string>(), 0, null);
        }
    }

    /// <summary>The audit log line for a load result.</summary>
    public static (string Level, string Message) Describe(LoadResult r)
    {
        if (!r.Exists)
            return ("INFO", $"Local settings file not found: {r.Path}. Using appsettings.json and the built-in defaults (create this file to override settings; restart the service to apply).");
        if (!r.Loaded)
            return ("ERROR", $"FAILED: local settings file {r.Path} could not be read and was IGNORED ({r.Error}). The service runs with appsettings.json and the built-in defaults. Fix the file and restart the service.");
        string list = r.Settings.Count == 0 ? "no settings (the file is empty)" : string.Join("; ", r.Settings);
        return ("INFO", $"Local settings file loaded: {r.Path} ({r.Bytes:N0} bytes, modified {r.ModifiedUtc:yyyy-MM-dd HH:mm:ss}Z), {r.Settings.Count} setting(s): {list}.");
    }

    private static readonly string[] SecretWords = { "password", "secret", "token", "apikey", "accesskey", "connectionstring", "instrumentationkey", "webhook" };

    private static void Flatten(JsonElement e, string prefix, List<string> into)
    {
        switch (e.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var p in e.EnumerateObject()) Flatten(p.Value, prefix.Length == 0 ? p.Name : prefix + ":" + p.Name, into);
                break;
            case JsonValueKind.Array:
                int i = 0;
                foreach (var item in e.EnumerateArray()) Flatten(item, $"{prefix}:{i++}", into);
                break;
            default:
                string lower = prefix.ToLowerInvariant();
                string value = SecretWords.Any(w => lower.Contains(w)) ? "********"
                    : e.ValueKind == JsonValueKind.String ? e.GetString() ?? string.Empty
                    : e.GetRawText();
                if (value.Length > 200) value = value.Substring(0, 197) + "...";
                into.Add($"{prefix} = {value}");
                break;
        }
    }
}
