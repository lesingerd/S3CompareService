namespace S3CompareService.Worker.Services;

/// <summary>
/// Records when the scheduler loop last completed an evaluation pass (it normally runs every 10 s). The IdleMon heartbeat reads it
/// to report "degraded" when the scheduler has stopped evaluating. Created at service start, so a scheduler that never completes
/// a pass is noticed too.
/// </summary>
public sealed class SchedulerHeartbeat(TimeProvider time)
{
    private long _lastTicks = time.GetUtcNow().UtcTicks;

    /// <summary>Gets the end of the last completed evaluation (or the service start before the first one).</summary>
    public DateTimeOffset LastEvaluation => new(Interlocked.Read(ref _lastTicks), TimeSpan.Zero);

    /// <summary>Called by the scheduler loop after each completed evaluation.</summary>
    public void Record() => Interlocked.Exchange(ref _lastTicks, time.GetUtcNow().UtcTicks);
}
