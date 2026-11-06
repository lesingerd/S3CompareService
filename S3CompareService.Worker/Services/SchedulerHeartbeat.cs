namespace S3CompareService.Worker.Services;

/// <summary>
/// Records when the scheduler loop last completed an evaluation pass (it normally runs every 10 s). The IdleMon heartbeat reads it
/// to report "degraded" when the scheduler has stopped evaluating. Created at service start, so a scheduler that never completes
/// a pass is noticed too. The age is measured on the monotonic clock, so a wall-clock step cannot hide a stall or fake one.
/// </summary>
public sealed class SchedulerHeartbeat(TimeProvider time)
{
    private long _lastTimestamp = time.GetTimestamp();

    /// <summary>Gets the end of the last completed evaluation (or the service start before the first one), as a time before now.</summary>
    public DateTimeOffset LastEvaluation
    {
        get
        {
            var age = time.GetElapsedTime(Interlocked.Read(ref _lastTimestamp), time.GetTimestamp());
            return time.GetUtcNow() - (age < TimeSpan.Zero ? TimeSpan.Zero : age);
        }
    }

    /// <summary>Called by the scheduler loop after each completed evaluation.</summary>
    public void Record() => Interlocked.Exchange(ref _lastTimestamp, time.GetTimestamp());
}
