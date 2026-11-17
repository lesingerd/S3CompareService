using Microsoft.Extensions.Logging.Abstractions;
using S3CompareService.Worker.IdleMon;
using static S3CompareService.Tests.IdleMonTestData;

namespace S3CompareService.Tests;

/// <summary>The self-reported process.* metrics: values, units, omission of unknowns, CPU percent and the collector wiring.</summary>
[TestClass]
public class IdleMonProcessMetricsTests : TestDbBase
{
    private sealed class FakeSource(Func<ProcessReading> read) : IProcessMetricsSource
    {
        public int Reads { get; private set; }

        public ProcessReading Read()
        {
            Reads++;
            return read();
        }
    }

    private static ProcessReading Full(TimeSpan? cpu = null) => new()
    {
        PrivateBytes = 200_000_000, WorkingSetBytes = 150_000_000, GcHeapBytes = 50_000_000, TotalAvailableMemoryBytes = 1_000_000_000,
        Handles = 321, Threads = 17, CpuTime = cpu ?? TimeSpan.FromSeconds(10), Uptime = TimeSpan.FromSeconds(3600.4), ProcessorCount = 4,
    };

    private static double Value(List<IdleMonMessage> list, string name) => list.Single(m => m.Name == name).Value!.Value;

    [TestMethod]
    public void Build_ReportsEveryValueWithItsUnit_AndNoCpuOnTheFirstSample()
    {
        var clock = new FakeClock();
        var list = new ProcessMetricsReader(new FakeSource(() => Full()), clock).Build(clock.GetUtcNow());

        Assert.AreEqual(200_000_000.0, Value(list, "process.memory.private_bytes"));
        Assert.AreEqual(150_000_000.0, Value(list, "process.memory.working_set"));
        Assert.AreEqual(50_000_000.0, Value(list, "process.gc.heap_bytes"));
        Assert.AreEqual(5.0, Value(list, "process.gc.heap_limit_percent"));
        Assert.AreEqual(321.0, Value(list, "process.handles"));
        Assert.AreEqual(17.0, Value(list, "process.threads"));
        Assert.AreEqual(3600.0, Value(list, "process.uptime"));
        Assert.IsFalse(list.Any(m => m.Name == "process.cpu.percent"), "the first sample has no CPU delta");
        Assert.AreEqual("bytes", list.Single(m => m.Name == "process.memory.private_bytes").Unit);
        Assert.AreEqual("percent", list.Single(m => m.Name == "process.gc.heap_limit_percent").Unit);
        Assert.AreEqual("count", list.Single(m => m.Name == "process.handles").Unit);
        Assert.AreEqual("seconds", list.Single(m => m.Name == "process.uptime").Unit);
        Assert.IsTrue(list.All(m => m.Tags is null));
    }

    [TestMethod]
    public void Build_CpuPercent_IsCpuDeltaOverWallDeltaOverProcessorCount()
    {
        var clock = new FakeClock();
        var cpu = TimeSpan.FromSeconds(10);
        var reader = new ProcessMetricsReader(new FakeSource(() => Full(cpu)), clock);
        reader.Build(clock.GetUtcNow());

        clock.Advance(TimeSpan.FromMinutes(5));
        cpu += TimeSpan.FromSeconds(120); // 120 s of CPU in 300 s on 4 processors = 10 %
        Assert.AreEqual(10.0, Value(reader.Build(clock.GetUtcNow()), "process.cpu.percent"), 0.001);

        // A wall clock step does not matter: only the monotonic clock is used.
        clock.StepWallClock(TimeSpan.FromHours(-5));
        clock.Advance(TimeSpan.FromMinutes(5));
        cpu += TimeSpan.FromSeconds(30);
        Assert.AreEqual(2.5, Value(reader.Build(clock.GetUtcNow()), "process.cpu.percent"), 0.001);
    }

    [TestMethod]
    public void Build_CpuPercent_IsOmittedWhenTheCpuTimeIsUnknown_AndTheNextSampleHasNoBaseline()
    {
        var clock = new FakeClock();
        TimeSpan? cpu = TimeSpan.FromSeconds(10);
        var reader = new ProcessMetricsReader(new FakeSource(() => Full() with { CpuTime = cpu }), clock);
        reader.Build(clock.GetUtcNow());

        clock.Advance(TimeSpan.FromMinutes(5));
        cpu = null;
        Assert.IsFalse(reader.Build(clock.GetUtcNow()).Any(m => m.Name == "process.cpu.percent"));

        clock.Advance(TimeSpan.FromMinutes(5));
        cpu = TimeSpan.FromSeconds(500);
        Assert.IsFalse(reader.Build(clock.GetUtcNow()).Any(m => m.Name == "process.cpu.percent"), "no baseline: not a value from an old sample");
    }

    [TestMethod]
    public void Build_CpuPercent_IsOmittedWhenNoTimeHasPassedOrTheCpuTimeWentBackwards()
    {
        var clock = new FakeClock();
        var cpu = TimeSpan.FromSeconds(10);
        var reader = new ProcessMetricsReader(new FakeSource(() => Full(cpu)), clock);
        reader.Build(clock.GetUtcNow());
        cpu += TimeSpan.FromSeconds(1);
        Assert.IsFalse(reader.Build(clock.GetUtcNow()).Any(m => m.Name == "process.cpu.percent"), "zero wall delta");

        clock.Advance(TimeSpan.FromMinutes(5));
        cpu = TimeSpan.FromSeconds(1);
        Assert.IsFalse(reader.Build(clock.GetUtcNow()).Any(m => m.Name == "process.cpu.percent"), "negative CPU delta");
    }

    [TestMethod]
    public void Build_OmitsWhatCannotBeRead_NeverZero()
    {
        var clock = new FakeClock();
        var reading = new ProcessReading { WorkingSetBytes = 5, Handles = 9 };
        var list = new ProcessMetricsReader(new FakeSource(() => reading), clock).Build(clock.GetUtcNow());

        CollectionAssert.AreEquivalent(new[] { "process.memory.working_set", "process.handles" }, list.Select(m => m.Name).ToList());
    }

    [TestMethod]
    public void Build_HeapPercent_IsOmittedWithoutAUsableLimit()
    {
        var clock = new FakeClock();
        var list = new ProcessMetricsReader(new FakeSource(() => Full() with { TotalAvailableMemoryBytes = 0 }), clock).Build(clock.GetUtcNow());
        Assert.IsTrue(list.Any(m => m.Name == "process.gc.heap_bytes"));
        Assert.IsFalse(list.Any(m => m.Name == "process.gc.heap_limit_percent"));

        list = new ProcessMetricsReader(new FakeSource(() => Full() with { Uptime = TimeSpan.FromSeconds(-1) }), clock).Build(clock.GetUtcNow());
        Assert.IsFalse(list.Any(m => m.Name == "process.uptime"), "a negative uptime is unknown");
    }

    [TestMethod]
    public void Build_NeverThrows_WhenTheSourceFails()
    {
        var clock = new FakeClock();
        var list = new ProcessMetricsReader(new FakeSource(() => throw new InvalidOperationException("no access")), clock).Build(clock.GetUtcNow());
        Assert.AreEqual(0, list.Count);
    }

    [TestMethod]
    public void CurrentProcessSource_ReadsRealValues_ThatPassTheContractValidation()
    {
        var clock = new FakeClock();
        var reader = new ProcessMetricsReader(new CurrentProcessMetricsSource(), clock);
        var reporter = new CapturingReporter(clock);
        var list = reader.Build(clock.GetUtcNow());
        foreach (var m in list) reporter.Submit(m);

        Assert.AreEqual(0, reporter.Invalid.Count, string.Join("; ", reporter.Invalid));
        foreach (var name in new[] { "process.memory.private_bytes", "process.memory.working_set", "process.gc.heap_bytes", "process.handles", "process.threads", "process.uptime" })
            Assert.IsTrue(Value(list, name) > 0, name);
    }

    [TestMethod]
    public async Task Collector_SendsProcessMetricsEachPass_AndNothingWithoutAReader()
    {
        var clock = new FakeClock();
        var reporter = new CapturingReporter(clock);
        var source = new FakeSource(() => Full());
        var collector = new IdleMonCollector(Options(), JobRepo, RunRepo, DiffRepo, new FakeScanSource(), reporter, null, () => clock.GetUtcNow(), clock,
            NullLogger<IdleMonCollector>.Instance, new ProcessMetricsReader(source, clock));

        await collector.CollectAsync(CancellationToken.None);
        Assert.AreEqual(1, source.Reads);
        Assert.AreEqual(1, reporter.Metrics("process.threads").Count());
        Assert.AreEqual(0, reporter.Invalid.Count, string.Join("; ", reporter.Invalid));

        var plain = new CapturingReporter(clock);
        await new IdleMonCollector(Options(), JobRepo, RunRepo, DiffRepo, new FakeScanSource(), plain, null, () => clock.GetUtcNow(), clock,
            NullLogger<IdleMonCollector>.Instance).CollectAsync(CancellationToken.None);
        Assert.IsFalse(plain.Messages.Any(m => m.Name?.StartsWith("process.", StringComparison.Ordinal) == true));
    }

    [TestMethod]
    public void Options_ProcessMetrics_IsOnByDefault()
    {
        Assert.IsTrue(new IdleMonOptions().ProcessMetrics);
    }
}
