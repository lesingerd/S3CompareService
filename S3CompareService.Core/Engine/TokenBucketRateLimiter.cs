namespace S3CompareService.Core.Engine;

/// <summary>Paces requests. <see cref="WaitAsync"/> returns when the caller may send one.</summary>
public interface IRequestRateLimiter
{
    /// <summary>Waits until one request is allowed.</summary>
    Task WaitAsync(CancellationToken cancellationToken);
}

/// <summary>
/// A token bucket on monotonic time (<see cref="TimeProvider.GetTimestamp"/>): it holds up to <c>burst</c> tokens (default: one
/// second's worth) and refills at <c>ratePerSecond</c>. A wall-clock step cannot make it stall or release a flood. Thread-safe,
/// so one instance can pace several concurrent callers (all partitions of a scan share the limiter of a bucket).
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

    /// <summary>The most tokens the bucket holds; a single acquire larger than this is capped to it.</summary>
    public double Capacity => _capacity;

    /// <summary>
    /// Takes a token when one is available (returns <see cref="TimeSpan.Zero"/>), otherwise returns how long to wait before
    /// trying again. Pure with respect to the clock, so tests can drive it with a manual <see cref="TimeProvider"/>.
    /// </summary>
    public TimeSpan TryAcquire() => TryAcquire(1);

    /// <summary>
    /// Takes <paramref name="tokens"/> tokens at once (capped to <see cref="Capacity"/>) when available, otherwise returns how
    /// long to wait. Lets a caller that does many cheap operations pay for them in batches.
    /// </summary>
    public TimeSpan TryAcquire(double tokens)
    {
        double need = Math.Min(Math.Max(tokens, 0), _capacity);
        lock (_gate)
        {
            long now = _time.GetTimestamp();
            double elapsedSeconds = _time.GetElapsedTime(_lastTimestamp, now).TotalSeconds;
            _lastTimestamp = now;
            if (elapsedSeconds > 0) _tokens = Math.Min(_capacity, _tokens + elapsedSeconds * _ratePerSecond);

            if (_tokens >= need)
            {
                _tokens -= need;
                return TimeSpan.Zero;
            }

            return TimeSpan.FromSeconds((need - _tokens) / _ratePerSecond);
        }
    }

    /// <inheritdoc />
    public Task WaitAsync(CancellationToken cancellationToken) => WaitAsync(1, cancellationToken);

    /// <summary>Waits until <paramref name="tokens"/> tokens (capped to <see cref="Capacity"/>) could be taken.</summary>
    public async Task WaitAsync(double tokens, CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var wait = TryAcquire(tokens);
            if (wait <= TimeSpan.Zero) return;
            await Task.Delay(wait, _time, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Creates a limiter for a configured rate, or null when the rate is 0 or less (unlimited).</summary>
    public static IRequestRateLimiter? Create(double ratePerSecond, TimeProvider? time = null) =>
        ratePerSecond > 0 ? new TokenBucketRateLimiter(ratePerSecond, time) : null;
}
