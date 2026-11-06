using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using S3CompareService.Core.Storage;

namespace S3CompareService.Worker.IdleMon;

/// <summary>
/// The hosted service that drives <see cref="IdleMonCollector"/>. It runs two independent loops, each on its own task: the
/// collection loop (the startup event, then a pass every metrics interval; it reads the database) and the heartbeat loop (a
/// heartbeat every heartbeat interval; it reads only memory). A database that blocks the collection loop therefore cannot delay
/// a heartbeat, and the heartbeat then says <c>degraded</c> because the collection made no progress. On a clean stop the
/// <c>stopping</c> heartbeat is sent. It is registered after the sender, so it stops first and the sender flushes what it
/// queued. It is only registered when reporting is enabled.
/// </summary>
public sealed class IdleMonReportingService(
    IdleMonCollector collector,
    IIdleMonEvents events,
    IJobRepository jobs,
    TimeProvider time,
    ILogger<IdleMonReportingService> logger) : BackgroundService
{
    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Task.Run: neither loop may block the host's startup, and a synchronous wait inside one stays inside its own task.
        var heartbeat = Task.Run(() => HeartbeatLoopAsync(stoppingToken), CancellationToken.None);
        var collection = Task.Run(() => CollectionLoopAsync(stoppingToken), CancellationToken.None);
        try
        {
            await Task.WhenAll(heartbeat, collection).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
    }

    private async Task HeartbeatLoopAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    collector.HeartbeatIfDue();
                }
                catch (Exception ex) when (ex is not OutOfMemoryException and not OperationCanceledException)
                {
                    logger.LogWarning("IdleMon heartbeat step failed ({Type}).", ex.GetType().Name);
                }

                await Task.Delay(collector.DelayUntilNextHeartbeat(), time, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
    }

    private async Task CollectionLoopAsync(CancellationToken stoppingToken)
    {
        try
        {
            int? jobCount = null;
            try
            {
                jobCount = (await jobs.GetAllAsync(stoppingToken).ConfigureAwait(false)).Count;
            }
            catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException)
            {
                // Unknown is not zero: the startup event just leaves the job count out.
            }

            events.ServiceStarted(jobCount);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await collector.CollectIfDueAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex) when (ex is not OutOfMemoryException)
                {
                    // Reporting must never hurt the service.
                    logger.LogWarning("IdleMon collection step failed ({Type}).", ex.GetType().Name);
                }

                await Task.Delay(collector.DelayUntilNextCollection(), time, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
        collector.SendStopping();
    }
}
