namespace S3CompareService.Core.Notifications;

/// <summary>Settings of the notification delivery (<c>S3Compare:Notifications:*</c>).</summary>
public sealed class NotificationOptions
{
    /// <summary>The default delivery timeout in seconds.</summary>
    public const int DefaultDeliveryTimeoutSeconds = 30;

    /// <summary>Gets or sets how long one delivery (SMTP, webhook or App Insights) may take before it is abandoned. Values below 1 mean the default.</summary>
    public int DeliveryTimeoutSeconds { get; set; } = DefaultDeliveryTimeoutSeconds;

    /// <summary>Gets the effective timeout.</summary>
    public TimeSpan Timeout => TimeSpan.FromSeconds(DeliveryTimeoutSeconds >= 1 ? DeliveryTimeoutSeconds : DefaultDeliveryTimeoutSeconds);
}
