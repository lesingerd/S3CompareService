using System.Diagnostics;
using System.Reflection;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Configuration;
using S3CompareService.Core.Engine;
using S3CompareService.Worker;

namespace S3CompareService.Tests;

/// <summary>Request rate limit (token bucket on monotonic time), process priority and the hardening settings (S-5).</summary>
[TestClass]
public class ScanLimitsTests
{
    /// <summary>A clock with a hand-moved monotonic timestamp (1 tick = 100 ns) and an unrelated wall clock.</summary>
    private sealed class ManualTime : TimeProvider
    {
        public long Timestamp { get; set; } = 5_000_000;
        public DateTimeOffset Wall { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        public override long GetTimestamp() => Timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override DateTimeOffset GetUtcNow() => Wall;
        public void Advance(TimeSpan by) => Timestamp += by.Ticks;
    }

    [TestMethod]
    public void TokenBucket_AllowsABurstOfOneSecond_ThenPacesAtTheRate()
    {
        var time = new ManualTime();
        var limiter = new TokenBucketRateLimiter(10, time); // 10 per second, bucket of 10

        for (var i = 0; i < 10; i++) Assert.AreEqual(TimeSpan.Zero, limiter.TryAcquire(), $"burst request {i}");
        var wait = limiter.TryAcquire();
        Assert.AreEqual(100, wait.TotalMilliseconds, 1.0, "the eleventh request waits for one token (1/10 s)");

        time.Advance(TimeSpan.FromMilliseconds(100));
        Assert.AreEqual(TimeSpan.Zero, limiter.TryAcquire());
        Assert.IsTrue(limiter.TryAcquire() > TimeSpan.Zero);

        // Steady state: one request per 100 ms is always allowed, never faster.
        var allowed = 0;
        for (var step = 0; step < 100; step++)
        {
            time.Advance(TimeSpan.FromMilliseconds(100));
            if (limiter.TryAcquire() == TimeSpan.Zero) allowed++;
        }

        Assert.AreEqual(100, allowed);
    }

    [TestMethod]
    public void TokenBucket_NeverHoldsMoreThanTheBurst_AfterALongIdle()
    {
        var time = new ManualTime();
        var limiter = new TokenBucketRateLimiter(5, time);
        time.Advance(TimeSpan.FromHours(3));
        var granted = 0;
        while (limiter.TryAcquire() == TimeSpan.Zero && granted < 100) granted++;
        Assert.AreEqual(5, granted, "an idle hour does not store an hour of requests");
    }

    [TestMethod]
    public void TokenBucket_IgnoresTheWallClock_AndABackwardsTimestamp()
    {
        var time = new ManualTime();
        var limiter = new TokenBucketRateLimiter(2, time);
        Assert.AreEqual(TimeSpan.Zero, limiter.TryAcquire());
        Assert.AreEqual(TimeSpan.Zero, limiter.TryAcquire());
        Assert.IsTrue(limiter.TryAcquire() > TimeSpan.Zero);

        time.Wall = time.Wall.AddDays(400); // wall clock jumps: no new tokens
        Assert.IsTrue(limiter.TryAcquire() > TimeSpan.Zero);

        time.Timestamp -= TimeSpan.FromSeconds(30).Ticks; // a timestamp that goes back is not a refill either
        Assert.IsTrue(limiter.TryAcquire() > TimeSpan.Zero);
        time.Advance(TimeSpan.FromSeconds(1)); // only this second counts (from the lowered timestamp: 1 s elapsed since the last look)
        Assert.AreEqual(TimeSpan.Zero, limiter.TryAcquire());
    }

    [TestMethod]
    public void TokenBucket_RejectsNonPositiveRates_AndCreateReturnsNullForUnlimited()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => new TokenBucketRateLimiter(0));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => new TokenBucketRateLimiter(double.NaN));
        Assert.IsNull(TokenBucketRateLimiter.Create(0));
        Assert.IsNull(TokenBucketRateLimiter.Create(-3));
        Assert.IsNotNull(TokenBucketRateLimiter.Create(20));
    }

    [TestMethod]
    public async Task TokenBucket_WaitAsync_ActuallyDelays_AndHonoursCancellation()
    {
        var limiter = new TokenBucketRateLimiter(100, burst: 1); // 10 ms per request after the first
        var sw = Stopwatch.StartNew();
        for (var i = 0; i < 11; i++) await limiter.WaitAsync(CancellationToken.None);
        Assert.IsTrue(sw.ElapsedMilliseconds >= 80, $"11 requests at 100/s took only {sw.ElapsedMilliseconds} ms");

        var slow = new TokenBucketRateLimiter(0.01, burst: 1); // 100 s per request
        await slow.WaitAsync(CancellationToken.None);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAsync<TaskCanceledException>(() => slow.WaitAsync(cts.Token));
    }

    // ---- the limiter is applied per list page ----

    private sealed class CountingLimiter : IRequestRateLimiter
    {
        public int Waits;
        public Task WaitAsync(CancellationToken cancellationToken) { Interlocked.Increment(ref Waits); return Task.CompletedTask; }
    }

    /// <summary>An IAmazonS3 that only answers ListObjectsV2 with pre-built pages (no network).</summary>
    public class ListingProxy : DispatchProxy
    {
        public int Requests;
        public int Pages = 3;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name != nameof(IAmazonS3.ListObjectsV2Async))
                throw new NotSupportedException(targetMethod?.Name);
            var request = (ListObjectsV2Request)args![0]!;
            var page = Interlocked.Increment(ref Requests);
            var response = new ListObjectsV2Response
            {
                IsTruncated = page < Pages,
                NextContinuationToken = page < Pages ? $"token{page}" : null,
                S3Objects = [new S3Object { Key = $"k{page}", Size = 1, ETag = "\"e\"", LastModified = DateTime.UtcNow }]
            };
            _ = request;
            return Task.FromResult(response);
        }
    }

    [TestMethod]
    public async Task PagingEnumerator_WaitsForTheLimiter_BeforeEveryListRequest()
    {
        var s3 = DispatchProxy.Create<IAmazonS3, ListingProxy>();
        var limiter = new CountingLimiter();
        await using var stream = new S3PagingEnumerator(s3, "bucket", "", rateLimiter: limiter);

        var keys = new List<string>();
        while (await stream.MoveNextAsync()) keys.Add(stream.Current!.Key);

        CollectionAssert.AreEqual(new[] { "k1", "k2", "k3" }, keys);
        Assert.AreEqual(3, limiter.Waits, "one wait per ListObjectsV2 request");
        Assert.AreEqual(3, ((ListingProxy)(object)s3).Requests);
    }

    [TestMethod]
    public async Task PagingEnumerator_WithoutALimiter_StillWorks()
    {
        var s3 = DispatchProxy.Create<IAmazonS3, ListingProxy>();
        await using var stream = new S3PagingEnumerator(s3, "bucket", "");
        var n = 0;
        while (await stream.MoveNextAsync()) n++;
        Assert.AreEqual(3, n);
    }

    // ---- settings ----

    private static IConfiguration Config(params (string Key, string Value)[] values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values.ToDictionary(v => v.Key, v => (string?)v.Value)).Build();

    [TestMethod]
    public void Settings_HaveTheDocumentedDefaults()
    {
        var empty = Config();
        var retention = HardeningSettings.Retention(empty);
        Assert.AreEqual(500, retention.KeepRunsPerJob);
        Assert.AreEqual(180, retention.KeepDays);
        Assert.AreEqual(90, retention.AuditLogDays);
        Assert.AreEqual(2000, retention.BatchSize);
        var scan = HardeningSettings.Scan(empty);
        Assert.AreEqual(100_000, scan.MaxRecordedDifferencesPerRun);
        Assert.AreEqual(20, scan.MaxListRequestsPerSecond);
        Assert.AreEqual(ProcessPriorityClass.BelowNormal, HardeningSettings.ParsePriority(empty[HardeningSettings.ProcessPriorityKey]));
    }

    [TestMethod]
    public void Settings_AreReadFromConfiguration_AndInvalidValuesFallBack()
    {
        var retention = HardeningSettings.Retention(Config(
            ("S3Compare:Retention:KeepRunsPerJob", "50"), ("S3Compare:Retention:KeepDays", "30"), ("S3Compare:Retention:AuditLogDays", "7")));
        Assert.AreEqual((50, 30, 7), (retention.KeepRunsPerJob, retention.KeepDays, retention.AuditLogDays));

        var negative = HardeningSettings.Retention(Config(("S3Compare:Retention:KeepRunsPerJob", "-4"), ("S3Compare:Retention:KeepDays", "-1")));
        Assert.AreEqual(500, negative.KeepRunsPerJob);
        Assert.AreEqual(180, negative.KeepDays);

        var scan = HardeningSettings.Scan(Config(("S3Compare:MaxRecordedDifferencesPerRun", "5000"), ("S3Compare:MaxListRequestsPerSecond", "2.5")));
        Assert.AreEqual(5000, scan.MaxRecordedDifferencesPerRun);
        Assert.AreEqual(2.5, scan.MaxListRequestsPerSecond);

        var unlimited = HardeningSettings.Scan(Config(("S3Compare:MaxRecordedDifferencesPerRun", "0"), ("S3Compare:MaxListRequestsPerSecond", "0")));
        Assert.AreEqual(0, unlimited.MaxRecordedDifferencesPerRun);
        Assert.AreEqual(0, unlimited.MaxListRequestsPerSecond);

        var junk = HardeningSettings.Scan(Config(("S3Compare:MaxRecordedDifferencesPerRun", "many"), ("S3Compare:MaxListRequestsPerSecond", "-5")));
        Assert.AreEqual(100_000, junk.MaxRecordedDifferencesPerRun);
        Assert.AreEqual(20, junk.MaxListRequestsPerSecond);
    }

    [TestMethod]
    public void ProcessPriority_ParsesNames_AndRefusesRealTime()
    {
        Assert.AreEqual(ProcessPriorityClass.BelowNormal, HardeningSettings.ParsePriority(null));
        Assert.AreEqual(ProcessPriorityClass.BelowNormal, HardeningSettings.ParsePriority(""));
        Assert.AreEqual(ProcessPriorityClass.BelowNormal, HardeningSettings.ParsePriority("nonsense"));
        Assert.AreEqual(ProcessPriorityClass.Idle, HardeningSettings.ParsePriority("idle"));
        Assert.AreEqual(ProcessPriorityClass.Normal, HardeningSettings.ParsePriority(" Normal "));
        Assert.AreEqual(ProcessPriorityClass.High, HardeningSettings.ParsePriority("HIGH"));
        Assert.AreEqual(ProcessPriorityClass.BelowNormal, HardeningSettings.ParsePriority("RealTime"), "RealTime can starve the host");
    }

    [TestMethod]
    public void ProcessPriority_IsAppliedToTheCurrentProcess()
    {
        using var process = Process.GetCurrentProcess();
        var original = process.PriorityClass;
        try
        {
            var applied = HardeningSettings.ApplyPriority("BelowNormal");
            Assert.AreEqual(ProcessPriorityClass.BelowNormal, applied);
            process.Refresh();
            Assert.AreEqual(ProcessPriorityClass.BelowNormal, process.PriorityClass);
        }
        finally
        {
            process.PriorityClass = original;
        }
    }
}
