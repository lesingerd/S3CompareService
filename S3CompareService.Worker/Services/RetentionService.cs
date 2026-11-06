using S3CompareService.Core.Storage;

namespace S3CompareService.Worker.Services;

/// <summary>
/// Runs <see cref="RetentionManager"/> once an hour (the first pass a few minutes after start, so start-up and crash recovery are
/// never delayed). Every iteration catches everything: a locked or failing database only means "try again in an hour". The wait
/// uses the monotonic clock, so a wall-clock step cannot make it spin or stall.
/// </summary>
public sealed class RetentionService : BackgroundService
{
    /// <summary>Time between passes.</summary>
    public static readonly TimeSpan Interval = TimeSpan.FromHours(1);

    /// <summary>Time from service start to the first pass.</summary>
    public static readonly TimeSpan FirstPassDelay = TimeSpan.FromMinutes(5);

    private readonly RetentionManager _manager;
    private readonly RetentionOptions _options;
    private readonly ScanExecutionCoordinator? _coordinator;
    private readonly TimeProvider _time;
    private readonly ILogger<RetentionService> _logger;
    private long _lastSuccessTimestamp;

    public RetentionService(RetentionManager manager, RetentionOptions options, TimeProvider time, ILogger<RetentionService> logger, ScanExecutionCoordinator? coordinator = null)
    {
        _manager = manager;
        _options = options.Normalized();
        _time = time;
        _logger = logger;
        _coordinator = coordinator;
    }

    /// <summary>The result of the last successful pass, or null.</summary>
    public RetentionResult? LastResult { get; private set; }

    /// <summary>Gets whether a pass has completed since the service started.</summary>
    public bool HasCompleted => Interlocked.Read(ref _lastSuccessTimestamp) != 0;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(FirstPassDelay, _time, stoppingToken).ConfigureAwait(false);
            while (!stoppingToken.IsCancellationRequested)
            {
                await RunPassAsync(stoppingToken).ConfigureAwait(false);
                await Task.Delay(Interval, _time, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
    }

    /// <summary>One iteration; never throws except for cancellation. Public so tests can drive it.</summary>
    public async Task RunPassAsync(CancellationToken cancellationToken)
    {
        try
        {
            // The one-time conversion of an old database locks writers while it runs: only when no scan is active.
            if (_coordinator is null || !_coordinator.GetActiveScans().Any())
            {
                var conversion = await _manager.EnsureIncrementalVacuumAsync(_options, DatabaseConfig.GetDefaultDatabasePath(), cancellationToken).ConfigureAwait(false);
                if (conversion == RetentionManager.VacuumConversion.Converted)
                    _logger.LogInformation("Database converted to incremental auto_vacuum (free space is now returned to the disk after retention).");
                else if (conversion == RetentionManager.VacuumConversion.SkippedTooLarge)
                    _logger.LogWarning("The database is larger than S3Compare:Retention:AutoVacuumConvertMaxMegabytes ({Limit} MB), so it was not converted to incremental auto_vacuum. Deleted history will not shrink the file until you stop the service and run: sqlite3 s3compareservice.db \"PRAGMA auto_vacuum = INCREMENTAL; VACUUM;\"", _options.AutoVacuumConvertMaxMegabytes);
                else if (conversion == RetentionManager.VacuumConversion.SkippedNoDiskSpace)
                    _logger.LogWarning("Not enough free disk space to convert the database to incremental auto_vacuum; will retry.");
            }

            var result = await _manager.RunOnceAsync(_options, DateTime.UtcNow, cancellationToken).ConfigureAwait(false);
            LastResult = result;
            Interlocked.Exchange(ref _lastSuccessTimestamp, Math.Max(1, _time.GetTimestamp()));
            if (result.RunsDeleted > 0 || result.AuditLogsDeleted > 0)
            {
                _logger.LogInformation("Retention removed {Runs} run(s) ({Differences} differences, {Snapshots} snapshots) and {Logs} audit log row(s); WAL checkpoint: {Checkpoint}.",
                    result.RunsDeleted, result.DifferencesDeleted, result.SnapshotsDeleted, result.AuditLogsDeleted, result.Checkpoint);
            }

            if (result.AgeRulesSkippedClockAhead)
                _logger.LogWarning("Retention skipped its age rules: the system clock is far ahead of the newest data. Check the clock.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Retention pass failed ({Type}: {Message}); trying again in an hour.", ex.GetType().Name, ex.Message);
        }
    }
}
