namespace S3CompareService.Core.Engine;

/// <summary>Paces requests. <see cref="WaitAsync"/> returns when the caller may send one.</summary>
public interface IRequestRateLimiter
{
    /// <summary>Waits until one request is allowed.</summary>
    Task WaitAsync(CancellationToken cancellationToken);
}

/// <summary>
/// A token bucket on monotonic time (<see cref="TimeProvider.GetTimestamp"/>): it holds up to <c>burst</c> tokens (default: one
/// second's worth) and refills at <c>ratePerSecond</c>. A wall-clock step cannot make it stall or release a flood. Thread-safe.
/// </summary>
public sealed class TokenBucketRateLimiter : IRequestRateLimiter
{
    private readonly double _ratePerSecond;
    private readonly double _capacity;
    private readonly TimeProvider _time;
    private readonly object _gate = new();
    private double _tokens;
    private long _lastTimestamp;

    /// <summary>Creates a limiter. The bucket starts full.</summary>
    public TokenBucketRateLimiter(double ratePerSecond, TimeProvider? time = null, double? burst = null)
    {
        if (ratePerSecond <= 0 || double.IsNaN(ratePerSecond) || double.IsInfinity(ratePerSecond))
            throw new ArgumentOutOfRangeException(nameof(ratePerSecond), "The rate must be a positive, finite number.");
        _ratePerSecond = ratePerSecond;
        _capacity = Math.Max(1, burst ?? ratePerSecond);
        _time = time ?? TimeProvider.System;
        _tokens = _capacity;
        _lastTimestamp = _time.GetTimestamp();
    }

    /// <summary>
    /// Takes a token when one is available (returns <see cref="TimeSpan.Zero"/>), otherwise returns how long to wait before
    /// trying again. Pure with respect to the clock, so tests can drive it with a manual <see cref="TimeProvider"/>.
    /// </summary>
    public TimeSpan TryAcquire()
    {
        lock (_gate)
        {
            long now = _time.GetTimestamp();
            double elapsedSeconds = _time.GetElapsedTime(_lastTimestamp, now).TotalSeconds;
            _lastTimestamp = now;
            if (elapsedSeconds > 0) _tokens = Math.Min(_capacity, _tokens + elapsedSeconds * _ratePerSecond);

            if (_tokens >= 1)
            {
                _tokens -= 1;
                return TimeSpan.Zero;
            }

            return TimeSpan.FromSeconds((1 - _tokens) / _ratePerSecond);
        }
    }

    /// <inheritdoc />
    public async Task WaitAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var wait = TryAcquire();
            if (wait <= TimeSpan.Zero) return;
            await Task.Delay(wait, _time, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Creates a limiter for a configured rate, or null when the rate is 0 or less (unlimited).</summary>
    public static IRequestRateLimiter? Create(double ratePerSecond, TimeProvider? time = null) =>
        ratePerSecond > 0 ? new TokenBucketRateLimiter(ratePerSecond, time) : null;
}
