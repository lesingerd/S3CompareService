using System.Reflection;
using Microsoft.Extensions.Logging;
using S3CompareService.Core.Storage;
using S3CompareService.Worker.Services;

namespace S3CompareService.Worker.IdleMon;

/// <summary>Reads the running scans from the coordinator.</summary>
public sealed class CoordinatorActiveScanSource(ScanExecutionCoordinator coordinator) : IActiveScanSource
{
    /// <inheritdoc />
    public IReadOnlyList<ActiveScanInfo> GetActiveScans() =>
        coordinator.GetActiveScans()
            .Select(s => new ActiveScanInfo(s.JobId, s.ScanRunId, s.LatestStats.ObjectsCompared, s.LatestStats.ItemsPerSecond, s.Engine.IsPaused))
            .ToList();
}

/// <summary>Registers the IdleMon reporter. With <c>IdleMon:Enabled</c> false it registers only no-ops and the status holder.</summary>
public static class IdleMonSetup
{
    /// <summary>Gets the application version for the wire (at most 32 characters, no build metadata).</summary>
    public static string Version()
    {
        var text = Assembly.GetEntryAssembly()?.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString()
            ?? "0.0.0";
        var plus = text.IndexOf('+');
        if (plus >= 0) text = text[..plus];
        return IdleMonLimits.Truncate(text, 32);
    }

    /// <summary>Registers everything. Call it before the scheduler is registered, so the sender stops last.</summary>
    public static IServiceCollection AddIdleMon(this IServiceCollection services, IConfiguration configuration)
    {
        var options = new IdleMonOptions();
        configuration.GetSection(IdleMonOptions.SectionName).Bind(options);
        var status = new IdleMonStatus();
        services.AddSingleton(options);
        services.AddSingleton(status);
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<SchedulerHeartbeat>();
        services.AddSingleton<IActiveScanSource, CoordinatorActiveScanSource>();

        var problem = options.Enabled ? options.Validate() : null;
        if (!options.Enabled || problem is not null)
        {
            status.ConfigProblem = problem;
            services.AddSingleton<IIdleMonReporter, NullIdleMonReporter>();
            services.AddSingleton<IIdleMonEvents>(NoOpIdleMonEvents.Instance);
            return services;
        }

        status.Enabled = true;
        var dryRun = options.ResolvedDryRunFile is not null;
        status.Mode = dryRun ? "DryRun" : "Http";

        var queue = new IdleMonQueue();
        status.Queue = queue;
        services.AddSingleton(queue);

        if (!dryRun)
        {
            services.AddSingleton(sp =>
            {
                var dir = string.IsNullOrWhiteSpace(options.SpoolDirectory)
                    ? Path.Combine(Path.GetDirectoryName(DatabaseConfig.GetDefaultDatabasePath())!, "idlemon-spool")
                    : Path.GetFullPath(options.SpoolDirectory.Trim().Trim('"'));
                var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("IdleMon.Spool");
                var spool = new IdleMonSpool(dir, options.SpoolMaxMegabytes * 1024L * 1024L, sp.GetRequiredService<TimeProvider>(), m => logger.LogWarning("{Message}", m));
                status.Spool = spool;
                return spool;
            });
        }

        services.AddSingleton<IIdleMonReporter, IdleMonReporter>();
        services.AddSingleton<IIdleMonEvents>(sp => new IdleMonEvents(sp.GetRequiredService<IIdleMonReporter>(), options, sp.GetRequiredService<TimeProvider>(), Version()));
        services.AddSingleton(sp => new IdleMonSender(
            options,
            queue,
            sp.GetService<IdleMonSpool>(),
            status,
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<IdleMonSender>>(),
            Environment.MachineName,
            Version()));
        services.AddSingleton(sp => new IdleMonCollector(
            options,
            sp.GetRequiredService<IJobRepository>(),
            sp.GetRequiredService<IScanRunRepository>(),
            sp.GetRequiredService<IDifferenceRepository>(),
            sp.GetRequiredService<IActiveScanSource>(),
            sp.GetRequiredService<IIdleMonReporter>(),
            sp.GetService<IdleMonSpool>(),
            () => sp.GetRequiredService<SchedulerHeartbeat>().LastEvaluation,
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<IdleMonCollector>>()));

        // Registration order is start order; the host stops them in reverse: reporting service first (stopping heartbeat), then the sender (final flush).
        services.AddHostedService(sp => sp.GetRequiredService<IdleMonSender>());
        services.AddHostedService<IdleMonReportingService>();
        return services;
    }
}
