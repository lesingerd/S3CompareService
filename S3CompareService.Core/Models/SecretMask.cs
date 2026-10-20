using System.Text.Json;

namespace S3CompareService.Core.Models;

/// <summary>
/// Helpers for API/UI boundaries: secrets are never sent back to clients; a fixed mask is sent instead and,
/// when a client posts the mask back unchanged, the previously stored value is kept.
/// </summary>
public static class SecretMask
{
    public const string Mask = "********";

    public static bool IsMask(string? value) => value == Mask;

    /// <summary>Returns the stored secret when the incoming value is the mask, otherwise the incoming value.</summary>
    public static string Merge(string? incoming, string? existing)
        => incoming == Mask ? (existing ?? string.Empty) : (incoming ?? string.Empty);

    private static string MaskIfSet(string? value) => string.IsNullOrEmpty(value) ? string.Empty : Mask;

    /// <summary>Deep copy of the job with every non-empty secret replaced by <see cref="Mask"/>.</summary>
    public static ComparisonJob MaskedCopy(ComparisonJob job)
    {
        var copy = Clone(job);
        copy.BucketA.AccessKey = MaskIfSet(copy.BucketA.AccessKey);
        copy.BucketA.SecretKey = MaskIfSet(copy.BucketA.SecretKey);
        copy.BucketB.AccessKey = MaskIfSet(copy.BucketB.AccessKey);
        copy.BucketB.SecretKey = MaskIfSet(copy.BucketB.SecretKey);
        copy.AlertConfig.SmtpPassword = MaskIfSet(copy.AlertConfig.SmtpPassword);
        copy.AlertConfig.AzureWebhookUrl = MaskIfSet(copy.AlertConfig.AzureWebhookUrl);
        copy.AlertConfig.AzureAppInsightsConnectionString = MaskIfSet(copy.AlertConfig.AzureAppInsightsConnectionString);
        return copy;
    }

    /// <summary>Same as <see cref="MaskedCopy(ComparisonJob)"/> for an alert config on its own.</summary>
    public static JobAlertConfig MaskedCopy(JobAlertConfig cfg)
    {
        var copy = Clone(cfg);
        copy.SmtpPassword = MaskIfSet(copy.SmtpPassword);
        copy.AzureWebhookUrl = MaskIfSet(copy.AzureWebhookUrl);
        copy.AzureAppInsightsConnectionString = MaskIfSet(copy.AzureAppInsightsConnectionString);
        return copy;
    }

    /// <summary>
    /// Replaces any masked secret in <paramref name="incoming"/> (in place) with the value from <paramref name="existing"/>.
    /// If there is no existing job the masked value becomes empty.
    /// </summary>
    public static void MergeMaskedSecrets(ComparisonJob incoming, ComparisonJob? existing)
    {
        incoming.BucketA.AccessKey = Merge(incoming.BucketA.AccessKey, existing?.BucketA.AccessKey);
        incoming.BucketA.SecretKey = Merge(incoming.BucketA.SecretKey, existing?.BucketA.SecretKey);
        incoming.BucketB.AccessKey = Merge(incoming.BucketB.AccessKey, existing?.BucketB.AccessKey);
        incoming.BucketB.SecretKey = Merge(incoming.BucketB.SecretKey, existing?.BucketB.SecretKey);
        MergeMaskedSecrets(incoming.AlertConfig, existing?.AlertConfig);
    }

    public static void MergeMaskedSecrets(JobAlertConfig incoming, JobAlertConfig? existing)
    {
        incoming.SmtpPassword = Merge(incoming.SmtpPassword, existing?.SmtpPassword);
        incoming.AzureWebhookUrl = Merge(incoming.AzureWebhookUrl, existing?.AzureWebhookUrl);
        incoming.AzureAppInsightsConnectionString = Merge(incoming.AzureAppInsightsConnectionString, existing?.AzureAppInsightsConnectionString);
    }

    internal static T Clone<T>(T value) where T : class
        => JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(value))!;
}
