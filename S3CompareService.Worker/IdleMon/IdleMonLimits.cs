using System.Security.Cryptography;
using System.Text;

namespace S3CompareService.Worker.IdleMon;

/// <summary>Length limits of the IdleMon contract for alert keys, titles and free text. The same helper is used for firing and resolved so keys agree.</summary>
public static class IdleMonLimits
{
    /// <summary>The longest alert key or title IdleMon accepts.</summary>
    public const int MaxKeyOrTitle = 200;

    /// <summary>Returns the key unchanged when it fits; otherwise 180 characters, "~" and 8 hex characters of its SHA-256 (stable across restarts).</summary>
    public static string CapKey(string key)
    {
        if (key.Length <= MaxKeyOrTitle) return key;
        var cut = 180;
        if (char.IsHighSurrogate(key[cut - 1])) cut--;
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        return key[..cut] + "~" + Convert.ToHexStringLower(hash.AsSpan(0, 4));
    }

    /// <summary>Returns the title unchanged when it fits, otherwise 199 characters and an ellipsis.</summary>
    public static string CapTitle(string title)
    {
        if (title.Length <= MaxKeyOrTitle) return title;
        var cut = MaxKeyOrTitle - 1;
        if (char.IsHighSurrogate(title[cut - 1])) cut--;
        return title[..cut] + "…";
    }

    /// <summary>Cuts a text to at most <paramref name="max"/> characters (never inside a surrogate pair).</summary>
    public static string Truncate(string value, int max)
    {
        if (value.Length <= max) return value;
        var cut = max;
        if (cut > 0 && char.IsHighSurrogate(value[cut - 1])) cut--;
        return value[..cut];
    }
}
