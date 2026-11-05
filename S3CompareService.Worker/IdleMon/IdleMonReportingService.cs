using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using S3CompareService.Core.Storage;

namespace S3CompareService.Worker.IdleMon;

/// <summary>
/// The hosted service that drives <see cref="IdleMonCollector"/>: the startup event, then a loop that collects every metrics
/// interval and heartbeats every heartbeat interval, and on a clean stop the <c>stopping</c> heartbeat. It is registered after
/// the sender, so it stops first and the sender flushes what it queued. It is only registered when reporting is enabled.
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
                    await collector.TickAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex) when (ex is not OutOfMemoryException)
                {
                    // Reporting must never hurt the service. No heartbeat is sent for a pass that threw before reaching it.
                    logger.LogWarning("IdleMon reporting step failed ({Type}).", ex.GetType().Name);
                }

                await Task.Delay(collector.DelayUntilNextTick(), time, stoppingToken).ConfigureAwait(false);
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
