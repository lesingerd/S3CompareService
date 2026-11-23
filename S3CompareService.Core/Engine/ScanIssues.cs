using System.Globalization;

namespace S3CompareService.Core.Engine;

/// <summary>
/// What an error meant for the scan, ordered from harmless to fatal. Only <see cref="Failed"/> is an error in the sense of
/// "the scan (or a range) stopped"; everything else is information about something that went wrong and was handled.
/// </summary>
public enum IssueOutcome
{
    /// <summary>A retried operation that then succeeded: no impact.</summary>
    Recovered = 0,
    /// <summary>A request was skipped (planner request, shared-prefix jump): the scan and its results are unaffected.</summary>
    Skipped = 1,
    /// <summary>Recorded against one object (for example a Content Check Error difference); the scan continued.</summary>
    Recorded = 2,
    /// <summary>Slower or less balanced, but still correct (planner time limit hit, planning failed so one range is scanned).</summary>
    Degraded = 3,
    /// <summary>The error stopped the scan (or a range).</summary>
    Failed = 4
}

/// <summary>One recorded issue, as shown in the banner, the diagnostics panel and /api/status.</summary>
public sealed class ScanIssueInfo
{
    /// <summary>UTC time, "HH:mm:ss".</summary>
    public string Time { get; set; } = string.Empty;
    /// <summary>The <see cref="IssueOutcome"/> name ("Recovered", "Skipped", "Recorded", "Degraded", "Failed").</summary>
    public string Outcome { get; set; } = nameof(IssueOutcome.Recovered);
    /// <summary>The attempt that succeeded, for a recovered issue (0 = not applicable).</summary>
    public int Attempt { get; set; }
    /// <summary>What happened and what it meant, without the outcome word.</summary>
    public string Text { get; set; } = string.Empty;

    /// <summary>The outcome as an enum (Recovered when the name is unknown, so a newer service never shows a red error by mistake).</summary>
    public IssueOutcome Kind => Enum.TryParse<IssueOutcome>(Outcome, out var k) ? k : IssueOutcome.Recovered;

    /// <summary>"RECOVERED (attempt 2): ..." / "SKIPPED: ..." / "FAILED: ...".</summary>
    public string Display => ScanIssues.Label(Kind, Attempt) + ": " + Text;

    /// <summary>A marker for the UI: ✓ recovered, ⓘ skipped, ▲ recorded or degraded, ✖ failed.</summary>
    public string Glyph => Kind switch
    {
        IssueOutcome.Recovered => "\u2713",
        IssueOutcome.Skipped => "\u24D8",
        IssueOutcome.Failed => "\u2716",
        _ => "\u25B2"
    };

    /// <summary>Time, outcome and text on one line (the "Recent issues" list).</summary>
    public string Line => $"{Time}Z  {Display}";
}

/// <summary>How many issues of each outcome a scan has had.</summary>
public sealed class IssueSummary
{
    public int Failed { get; set; }
    public int Degraded { get; set; }
    public int Recorded { get; set; }
    public int Skipped { get; set; }
    public int Recovered { get; set; }

    public int Total => Failed + Degraded + Recorded + Skipped + Recovered;

    /// <summary>"Issues: 0 failed · 2 recovered · 1 skipped · 0 degraded" (plus "· N recorded" when there are any).</summary>
    public string Text =>
        $"Issues: {Failed} failed · {Recovered} recovered · {Skipped} skipped · {Degraded} degraded" + (Recorded > 0 ? $" · {Recorded} recorded" : string.Empty);
}

/// <summary>
/// The classified errors of one scan: counts per outcome, the most recent issue of each outcome and the last few issues.
/// Bounded (a scan with thousands of skipped requests keeps counting but stores almost nothing) and thread-safe.
/// </summary>
public sealed class ScanIssues
{
    /// <summary>Issues kept for the "Recent issues" list.</summary>
    public const int MaxRecent = 10;
    /// <summary>Longest issue text kept.</summary>
    public const int MaxTextLength = 400;
    /// <summary>Audit log lines written per outcome; further issues are counted and summarised once.</summary>
    public const int MaxLoggedPerOutcome = 20;

    private static readonly int OutcomeCount = Enum.GetValues<IssueOutcome>().Length;

    private readonly object _gate = new();
    private readonly long[] _counts = new long[OutcomeCount];
    private readonly ScanIssueInfo?[] _lastOfKind = new ScanIssueInfo?[OutcomeCount];
    private readonly long[] _logged = new long[OutcomeCount];
    private readonly LinkedList<ScanIssueInfo> _recent = new();

    /// <summary>Audit log sink: (level, message). WARN for everything but Failed, which is ERROR.</summary>
    public Action<string, string>? Log { get; set; }

    public static string Label(IssueOutcome outcome, int attempt = 0) =>
        outcome.ToString().ToUpperInvariant() + (outcome == IssueOutcome.Recovered && attempt > 1 ? $" (attempt {attempt})" : string.Empty);

    public static string Clip(string text) => text.Length <= MaxTextLength ? text : text.Substring(0, MaxTextLength - 3) + "...";

    /// <summary>Records one issue. <paramref name="log"/> = false when the caller writes its own audit log line.</summary>
    public void Record(IssueOutcome outcome, string text, int attempt = 0, bool log = true)
    {
        var info = new ScanIssueInfo
        {
            Time = DateTime.UtcNow.ToString("HH:mm:ss", CultureInfo.InvariantCulture),
            Outcome = outcome.ToString(),
            Attempt = attempt,
            Text = Clip(text ?? string.Empty)
        };
        long loggedBefore = 0;
        lock (_gate)
        {
            _counts[(int)outcome]++;
            _lastOfKind[(int)outcome] = info;
            _recent.AddFirst(info);
            while (_recent.Count > MaxRecent) _recent.RemoveLast();
            if (log) loggedBefore = _logged[(int)outcome]++;
        }
        if (!log || Log == null) return;
        string level = outcome == IssueOutcome.Failed ? "ERROR" : "WARN";
        try
        {
            if (loggedBefore < MaxLoggedPerOutcome) Log(level, info.Display);
            else if (loggedBefore == MaxLoggedPerOutcome)
                Log(level, $"{outcome.ToString().ToUpperInvariant()}: further {outcome.ToString().ToLowerInvariant()} issues are counted but not logged individually.");
        }
        catch { /* the audit log must never break a scan */ }
    }

    public IssueSummary Summary()
    {
        lock (_gate)
        {
            return new IssueSummary
            {
                Failed = (int)_counts[(int)IssueOutcome.Failed],
                Degraded = (int)_counts[(int)IssueOutcome.Degraded],
                Recorded = (int)_counts[(int)IssueOutcome.Recorded],
                Skipped = (int)_counts[(int)IssueOutcome.Skipped],
                Recovered = (int)_counts[(int)IssueOutcome.Recovered]
            };
        }
    }

    /// <summary>The most severe issue (Failed, Degraded, Recorded, Skipped, Recovered); among equals the most recent. Null when there is none.</summary>
    public ScanIssueInfo? Top()
    {
        lock (_gate)
        {
            for (int i = OutcomeCount - 1; i >= 0; i--)
                if (_lastOfKind[i] != null) return Copy(_lastOfKind[i]!);
            return null;
        }
    }

    /// <summary>The last issues, newest first.</summary>
    public List<ScanIssueInfo> Recent()
    {
        lock (_gate) return _recent.Select(Copy).ToList();
    }

    private static ScanIssueInfo Copy(ScanIssueInfo i) => new() { Time = i.Time, Outcome = i.Outcome, Attempt = i.Attempt, Text = i.Text };
}
