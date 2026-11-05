using System.Windows.Media;
using S3CompareService.Core.IdleMon;

namespace S3CompareService.UI.ViewModels;

/// <summary>Everything the IdleMon card shows, worked out from one status reply. Immutable; the state logic is in <see cref="IdleMonCard"/>.</summary>
public sealed class IdleMonCardView
{
    private static SolidColorBrush Frozen(byte r, byte g, byte b)
    {
        var brush = new SolidColorBrush(Color.FromRgb(r, g, b));
        brush.Freeze();
        return brush;
    }

    private static readonly Brush Grey = Frozen(62, 62, 66);       // #3E3E42, as the DISABLED badge
    private static readonly Brush Blue = Frozen(86, 156, 214);      // #569CD6
    private static readonly Brush Green = Frozen(129, 199, 132);    // #81C784
    private static readonly Brush Amber = Frozen(220, 180, 80);     // #DCB450
    private static readonly Brush Red = Frozen(244, 135, 113);      // #F48771
    private static readonly Brush Dark = Frozen(30, 30, 30);        // #1E1E1E
    private static readonly Brush Light = Frozen(204, 204, 204);    // #CCCCCC

    public const string EnableHint = @"Enable with scripts\configure-idlemon.ps1 -Enable -Restart";

    public IdleMonCardState State { get; init; } = IdleMonCardState.Unknown;
    public string BadgeText { get; init; } = "Unknown";
    public Brush BadgeBrush { get; init; } = Grey;
    public Brush BadgeForeground { get; init; } = Light;

    /// <summary>First detail line (last send, counters), or empty.</summary>
    public string Details { get; init; } = "";

    /// <summary>Second line: the reason or file (config problem, dry-run file, spool size, last error), or empty.</summary>
    public string Notice { get; init; } = "";

    /// <summary>Hint line with the settings file and how to enable.</summary>
    public string Hint { get; init; } = "";

    public static IdleMonCardView From(IdleMonStatusDto? s, DateTime utcNow)
    {
        var state = IdleMonCard.Derive(s, utcNow);
        var (bg, fg) = state switch
        {
            IdleMonCardState.Sending => (Green, Dark),
            IdleMonCardState.Spooling or IdleMonCardState.Waiting => (Amber, Dark),
            IdleMonCardState.ConfigProblem => (Red, Dark),
            IdleMonCardState.DryRun => (Blue, Dark),
            _ => (Grey, Light),
        };

        if (s is null)
            return new IdleMonCardView { State = state, BadgeText = IdleMonCard.Label(state), BadgeBrush = bg, BadgeForeground = fg, Notice = "The service is not reachable." };

        string details = "";
        if (state is not (IdleMonCardState.Off or IdleMonCardState.ConfigProblem))
        {
            string last = s.LastSendAt is { } t
                ? $"{IdleMonCard.Relative(t, utcNow)} ({DateTime.SpecifyKind(t, DateTimeKind.Utc):yyyy-MM-dd HH:mm:ss} UTC)"
                : "never";
            details = $"Last send: {last}   Accepted {s.Accepted:N0}   Rejected {s.Rejected:N0}   Dropped {s.Dropped:N0}   "
                    + $"Queue {s.QueueLength:N0}   API key: {(s.ApiKeyPresent ? "yes" : "no")}";
        }

        string notice = state switch
        {
            IdleMonCardState.ConfigProblem => s.ConfigProblem ?? "",
            IdleMonCardState.DryRun => "Writing to " + (s.DryRunFile.Length > 0 ? s.DryRunFile : "a file") + " instead of sending.",
            IdleMonCardState.Spooling => $"Spool: {IdleMonCard.Size(s.SpoolBytes)} in {s.SpoolFiles:N0} file(s)." + (string.IsNullOrWhiteSpace(s.LastError) ? "" : " " + s.LastError),
            IdleMonCardState.Waiting => "Enabled, nothing sent yet.",
            _ => "",
        };

        string hint = s.SettingsFile.Length > 0
            ? $"Settings: {s.SettingsFile}{(s.SettingsFileExists ? "" : " (not created yet)")} (restart the service after editing)"
            : "";
        if (state == IdleMonCardState.Off)
            hint = hint.Length > 0 ? hint + ". " + EnableHint : EnableHint;

        return new IdleMonCardView { State = state, BadgeText = IdleMonCard.Label(state), BadgeBrush = bg, BadgeForeground = fg, Details = details, Notice = notice, Hint = hint };
    }
}
