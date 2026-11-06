using System.Net;
using System.Text;
using S3CompareService.Core.Models;
using S3CompareService.Worker.IdleMon;

namespace S3CompareService.Tests;

/// <summary>
/// A clock the test moves by hand (no extra package needed). It has two independent readings, like the real one: the wall clock
/// (<see cref="GetUtcNow"/>) and a monotonic timestamp (<see cref="GetTimestamp"/>, 1 tick = 100 ns). <see cref="Advance"/> moves
/// both; <see cref="StepWallClock"/> moves only the wall clock, like an administrator or a time sync changing the time.
/// </summary>
public sealed class FakeClock : TimeProvider
{
    private DateTimeOffset _now;
    private long _monotonic = 1_000_000;

    public FakeClock(DateTimeOffset? start = null) => _now = start ?? new DateTimeOffset(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => _now;

    public override long GetTimestamp() => Interlocked.Read(ref _monotonic);

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public void Advance(TimeSpan by)
    {
        _now += by;
        Interlocked.Add(ref _monotonic, by.Ticks);
    }

    /// <summary>Moves only the wall clock (forwards or backwards); the monotonic clock does not notice.</summary>
    public void StepWallClock(TimeSpan by) => _now += by;

    public DateTime UtcNow => _now.UtcDateTime;
}

/// <summary>Collects what the builders and the collector submit, and checks every message against the contract rules.</summary>
public sealed class CapturingReporter(TimeProvider time) : IIdleMonReporter
{
    public List<IdleMonMessage> Messages { get; } = [];

    public List<string> Invalid { get; } = [];

    public bool IsEnabled => true;

    public void Submit(IdleMonMessage message)
    {
        var problem = IdleMonValidator.Validate(message, time.GetUtcNow(), requireId: false);
        if (problem is not null) Invalid.Add($"{message.Kind} {message.Name ?? message.Key}: {problem}");
        Messages.Add(message);
    }

    public IEnumerable<IdleMonMessage> Metrics(string name) => Messages.Where(m => m.Kind == "metric" && m.Name == name);

    public IEnumerable<IdleMonMessage> Alerts(string? key = null) => Messages.Where(m => m.Kind == "alert" && (key is null || m.Key == key));

    public IdleMonMessage? LastHeartbeat => Messages.LastOrDefault(m => m.Kind == "heartbeat");
}

public sealed class ThrowingReporter : IIdleMonReporter
{
    public bool IsEnabled => true;

    public void Submit(IdleMonMessage message) => throw new InvalidOperationException("reporter is broken");
}

public sealed class ThrowingEvents : IIdleMonEvents
{
    public void RunCompleted(ComparisonJob job, ScanRun run) => throw new InvalidOperationException("boom");
    public void RunFailed(ComparisonJob job, ScanRun run, Exception error) => throw new InvalidOperationException("boom");
    public void RunCancelled(ComparisonJob job, ScanRun run) => throw new InvalidOperationException("boom");
    public void RunRecovered(ScanRun run, bool resumed) => throw new InvalidOperationException("boom");
    public void JobChanged(string action, ComparisonJob job) => throw new InvalidOperationException("boom");
    public void ServiceStarted(int? jobCount) => throw new InvalidOperationException("boom");
}

public sealed class FakeScanSource : IActiveScanSource
{
    public List<ActiveScanInfo> Scans { get; } = [];

    public IReadOnlyList<ActiveScanInfo> GetActiveScans() => Scans.ToList();
}

/// <summary>An HTTP handler that records requests and answers from a delegate. Nothing leaves the process.</summary>
public sealed class FakeHandler(Func<HttpRequestMessage, HttpStatusCode> respond) : HttpMessageHandler
{
    public List<(Uri? Uri, HttpMethod Method, string Body, IReadOnlyDictionary<string, string> Headers)> Requests { get; } = [];

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
        var headers = request.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value));
        Requests.Add((request.RequestUri, request.Method, body, headers));
        var status = respond(request);
        return new HttpResponseMessage(status) { Content = new StringContent("{\"accepted\":1,\"duplicates\":0,\"rejected\":[]}", Encoding.UTF8, "application/json") };
    }
}

public static class IdleMonTestData
{
    public static IdleMonOptions Options(Action<IdleMonOptions>? configure = null)
    {
        var o = new IdleMonOptions { Enabled = true, MetricsIntervalSeconds = 300, HeartbeatIntervalSeconds = 60 };
        configure?.Invoke(o);
        return o;
    }

    public static ComparisonJob Job(string id = "job1", string name = "Archive vs Wasabi", int intervalMinutes = 60, bool enabled = true, DateTime? createdAt = null) => new()
    {
        Id = id,
        Name = name,
        IsEnabled = enabled,
        ScheduleType = JobScheduleType.IntervalMinutes,
        IntervalMinutes = intervalMinutes,
        BucketA = { BucketName = "contoso-archive", EndpointUrl = "https://s3.us-east-1.amazonaws.com" },
        BucketB = { BucketName = "contoso-archive-copy", EndpointUrl = "https://s3.wasabisys.com" },
        CreatedAtUtc = createdAt ?? new DateTime(2026, 11, 5, 11, 30, 0, DateTimeKind.Utc), // 30 minutes before the usual test clock (12:00)
    };

    public static ScanRun Run(string jobId, RunStatus status, DateTime start, TimeSpan? duration = null, Action<ScanRun>? configure = null)
    {
        var d = duration ?? TimeSpan.FromMinutes(10);
        var run = new ScanRun
        {
            Id = Guid.NewGuid().ToString("N"),
            JobId = jobId,
            JobName = "Archive vs Wasabi",
            StartTimeUtc = start,
            EndTimeUtc = status is RunStatus.Running or RunStatus.Paused ? null : start + d,
            Status = status,
            DurationSeconds = d.TotalSeconds,
            ObjectsCompared = 1000,
            MatchedCount = 990,
            FilteredDifferencesCount = 0,
            BucketABytesRead = 100,
            BucketBBytesRead = 50,
        };
        configure?.Invoke(run);
        return run;
    }

    /// <summary>Builds a job state; runs are sorted newest start first, as the repository returns them.</summary>
    public static JobState State(ComparisonJob job, ActiveScanInfo? scan = null, params ScanRun[] runs) =>
        new(job, runs.OrderByDescending(r => r.StartTimeUtc).ToList(), scan);
}
