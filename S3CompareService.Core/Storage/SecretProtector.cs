using System.Security.Cryptography;
using System.Text;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Storage;

/// <summary>
/// Encrypts secrets at rest with DPAPI (LocalMachine scope) so the Worker service and the UI (any user on the
/// machine) can both read them. Stored values carry a "dpapi:" prefix; values without the prefix are treated as
/// legacy plaintext and are encrypted on their next save. On non-Windows platforms values are stored as-is.
/// </summary>
public static class SecretProtector
{
    public const string Prefix = "dpapi:";
    private static readonly byte[] Entropy = Encoding.UTF8.GetBytes("S3CompareService.v1");

    public static string Protect(string? plain)
    {
        if (string.IsNullOrEmpty(plain)) return string.Empty;
        if (plain.StartsWith(Prefix, StringComparison.Ordinal)) return plain; // already protected
        if (!OperatingSystem.IsWindows()) return plain;

        byte[] cipher = ProtectedData.Protect(Encoding.UTF8.GetBytes(plain), Entropy, DataProtectionScope.LocalMachine);
        return Prefix + Convert.ToBase64String(cipher);
    }

    public static string Unprotect(string? stored)
    {
        if (string.IsNullOrEmpty(stored)) return string.Empty;
        if (!stored.StartsWith(Prefix, StringComparison.Ordinal)) return stored; // legacy plaintext
        if (!OperatingSystem.IsWindows())
            throw new PlatformNotSupportedException("DPAPI-protected secrets can only be read on Windows.");

        byte[] plain = ProtectedData.Unprotect(Convert.FromBase64String(stored.Substring(Prefix.Length)), Entropy, DataProtectionScope.LocalMachine);
        return Encoding.UTF8.GetString(plain);
    }

    public static bool IsProtected(string? stored) => stored != null && stored.StartsWith(Prefix, StringComparison.Ordinal);

    internal static void Transform(S3BucketConfig cfg, Func<string?, string> f)
    {
        cfg.AccessKey = f(cfg.AccessKey);
        cfg.SecretKey = f(cfg.SecretKey);
    }

    internal static void Transform(JobAlertConfig cfg, Func<string?, string> f)
    {
        cfg.SmtpPassword = f(cfg.SmtpPassword);
        cfg.AzureWebhookUrl = f(cfg.AzureWebhookUrl);
        cfg.AzureAppInsightsConnectionString = f(cfg.AzureAppInsightsConnectionString);
    }
}
