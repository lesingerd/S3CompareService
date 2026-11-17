using System.Diagnostics;

namespace S3CompareService.Worker.IdleMon;

/// <summary>
/// One reading of this process. Every value is nullable: a value that could not be read is <c>null</c> and is left out of the
/// report (unknown is not zero).
/// </summary>
public sealed record ProcessReading
{
    /// <summary>Gets the private bytes of the process.</summary>
    public long? PrivateBytes { get; init; }

    /// <summary>Gets the working set of the process in bytes.</summary>
    public long? WorkingSetBytes { get; init; }

    /// <summary>Gets the size of the managed heap at the last GC, in bytes.</summary>
    public long? GcHeapBytes { get; init; }

    /// <summary>Gets the memory the GC may use (<c>GCMemoryInfo.TotalAvailableMemoryBytes</c>).</summary>
    public long? TotalAvailableMemoryBytes { get; init; }

    /// <summary>Gets the number of open handles.</summary>
    public int? Handles { get; init; }

    /// <summary>Gets the number of threads.</summary>
    public int? Threads { get; init; }

    /// <summary>Gets the total processor time the process has used.</summary>
    public TimeSpan? CpuTime { get; init; }

    /// <summary>Gets how long the process has been running.</summary>
    public TimeSpan? Uptime { get; init; }

    /// <summary>Gets the number of logical processors.</summary>
    public int ProcessorCount { get; init; } = 1;
}

/// <summary>Where <see cref="ProcessMetricsReader"/> gets its numbers from. Injectable so tests need no real process.</summary>
public interface IProcessMetricsSource
{
    /// <summary>Reads the process now. Must be cheap and must not wait for anything; it may throw, the reader then reports nothing.</summary>
    ProcessReading Read();
}

/// <summary>The real source: the current process and the GC. Each value is read on its own, so one that fails does not hide the others.</summary>
public sealed class CurrentProcessMetricsSource : IProcessMetricsSource
{
    /// <inheritdoc />
    public ProcessReading Read()
    {
        long? privateBytes = null, workingSet = null;
        int? handles = null, threads = null;
        TimeSpan? cpu = null, uptime = null;

        try
        {
            using var process = Process.GetCurrentProcess();
            privateBytes = Try(() => process.PrivateMemorySize64);
            workingSet = Try(() => process.WorkingSet64);
            handles = Try(() => process.HandleCount);
            threads = Try(() => CountThreads(process));
            cpu = Try(() => process.TotalProcessorTime);
            uptime = Try(() => DateTime.Now - process.StartTime);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // The process object itself could not be had; everything stays unknown.
        }

        long? heap = null, available = null;
        try
        {
            var info = GC.GetGCMemoryInfo();
            heap = info.HeapSizeBytes;
            available = info.TotalAvailableMemoryBytes;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Unknown.
        }

        return new ProcessReading
        {
            PrivateBytes = privateBytes,
            WorkingSetBytes = workingSet,
            GcHeapBytes = heap,
            TotalAvailableMemoryBytes = available,
            Handles = handles,
            Threads = threads,
            CpuTime = cpu,
            Uptime = uptime,
            ProcessorCount = Math.Max(1, Environment.ProcessorCount),
        };
    }

    // ProcessThread is a Component: each one is disposed.
    private static int CountThreads(Process process)
    {
        var threads = process.Threads;
        var count = threads.Count;
        foreach (ProcessThread thread in threads) thread.Dispose();
        return count;
    }

    private static T? Try<T>(Func<T> read) where T : struct
    {
        try
        {
            return read();
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return null;
        }
    }
}

/// <summary>
/// Builds the self-reported <c>process.*</c> metrics. CPU percent needs two samples: it is the process CPU time delta divided by the
/// wall delta (monotonic clock) and the processor count, so the first sample leaves it out. Keep one instance and call it from one loop.
/// </summary>
public sealed class ProcessMetricsReader(IProcessMetricsSource source, TimeProvider time)
{
    private TimeSpan? _lastCpu;
    private long _lastTimestamp;

    /// <summary>Builds the metrics for one sample. Never throws; a value that cannot be read is omitted.</summary>
    public List<IdleMonMessage> Build(DateTimeOffset now)
    {
        var list = new List<IdleMonMessage>();
        ProcessReading r;
        try
        {
            r = source.Read();
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return list;
        }

        if (r.PrivateBytes is >= 0) list.Add(IdleMonMessage.Metric(now, "process.memory.private_bytes", r.PrivateBytes.Value, "bytes"));
        if (r.WorkingSetBytes is >= 0) list.Add(IdleMonMessage.Metric(now, "process.memory.working_set", r.WorkingSetBytes.Value, "bytes"));
        if (r.GcHeapBytes is >= 0)
        {
            list.Add(IdleMonMessage.Metric(now, "process.gc.heap_bytes", r.GcHeapBytes.Value, "bytes"));
            if (r.TotalAvailableMemoryBytes is > 0)
                list.Add(IdleMonMessage.Metric(now, "process.gc.heap_limit_percent", Math.Round(r.GcHeapBytes.Value * 100.0 / r.TotalAvailableMemoryBytes.Value, 2), "percent"));
        }

        if (r.Handles is >= 0) list.Add(IdleMonMessage.Metric(now, "process.handles", r.Handles.Value, "count"));
        if (r.Threads is >= 0) list.Add(IdleMonMessage.Metric(now, "process.threads", r.Threads.Value, "count"));

        if (Cpu(r) is { } cpu) list.Add(IdleMonMessage.Metric(now, "process.cpu.percent", cpu, "percent"));
        if (r.Uptime is { } up && up >= TimeSpan.Zero) list.Add(IdleMonMessage.Metric(now, "process.uptime", Math.Round(up.TotalSeconds, 0), "seconds"));
        return list;
    }

    private double? Cpu(ProcessReading r)
    {
        var ts = time.GetTimestamp();
        var previousCpu = _lastCpu;
        var previousTs = _lastTimestamp;
        if (r.CpuTime is null)
        {
            // Unknown: no value now, and the next one has no usable baseline either.
            _lastCpu = null;
            return null;
        }

        _lastCpu = r.CpuTime;
        _lastTimestamp = ts;
        if (previousCpu is null) return null;

        var wall = time.GetElapsedTime(previousTs, ts).TotalSeconds;
        var cpu = (r.CpuTime.Value - previousCpu.Value).TotalSeconds;
        if (wall <= 0 || cpu < 0) return null;
        var percent = cpu / wall / Math.Max(1, r.ProcessorCount) * 100.0;
        return double.IsFinite(percent) ? Math.Round(percent, 2) : null;
    }
}
