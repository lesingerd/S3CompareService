using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace S3CompareService.Worker.IdleMon;

/// <summary>What happened to one envelope.</summary>
internal enum SendOutcome
{
    /// <summary>Accepted (rejected messages inside it are counted but never retried).</summary>
    Done,

    /// <summary>Not delivered, try again later (the caller spools it).</summary>
    Retry,

    /// <summary>Given up: IdleMon will never accept it.</summary>
    Dropped,
}

/// <summary>
/// The background sender. Every two seconds (or as soon as 200 messages wait) it sends what the reporter queued to the
/// local IdleMon agent, replaying spooled files first (oldest first). On a retryable failure the batch goes to the spool and
/// the sender backs off (1 s doubling to 60 s, with jitter). In dry-run mode it appends each envelope to a file instead.
/// It uses its own <see cref="HttpClient"/> behind <see cref="LoopbackOnlyHandler"/>.
/// </summary>
public sealed class IdleMonSender : BackgroundService
{
    /// <summary>The flush interval.</summary>
    public static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(2);

    /// <summary>The most messages in one batch.</summary>
    public const int MaxBatchMessages = 200;

    /// <summary>The longest backoff.</summary>
    public static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(60);

    private readonly IdleMonOptions _options;
    private readonly IdleMonQueue _queue;
    private readonly IdleMonSpool? _spool;
    private readonly IdleMonStatus _status;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly string _host;
    private readonly string _version;
    private readonly HttpClient? _http;
    private readonly Uri? _ingest;
    private readonly string? _dryRunFile;
    private readonly Random _random = new();
    private TimeSpan _backoff = TimeSpan.Zero;
    private DateTimeOffset _nextAttempt = DateTimeOffset.MinValue; // for display; the decision uses the monotonic _nextAttemptTimestamp
    private long _nextAttemptTimestamp; // 0 = no backoff
    private DateTimeOffset _lastUnauthorizedLog = DateTimeOffset.MinValue;

    /// <summary>Creates the sender. <paramref name="innerHandler"/> replaces the real network handler (tests).</summary>
    public IdleMonSender(
        IdleMonOptions options,
        IdleMonQueue queue,
        IdleMonSpool? spool,
        IdleMonStatus status,
        TimeProvider time,
        ILogger<IdleMonSender> logger,
        string host,
        string version,
        HttpMessageHandler? innerHandler = null)
    {
        _options = options;
        _queue = queue;
        _spool = spool;
        _status = status;
        _time = time;
        _logger = logger;
        _host = host;
        _version = version;
        _dryRunFile = options.ResolvedDryRunFile;
        if (_dryRunFile is null)
        {
            var baseUri = new Uri(options.AgentUrl.TrimEnd('/') + "/");
            _ingest = new Uri(baseUri, "api/v1/ingest");
            var inner = innerHandler ?? new SocketsHttpHandler
            {
                AllowAutoRedirect = false,
                UseProxy = false,
                UseCookies = false,
                ConnectTimeout = TimeSpan.FromSeconds(3),
            };
            _http = new HttpClient(new LoopbackOnlyHandler(inner, baseUri), disposeHandler: true) { Timeout = TimeSpan.FromSeconds(10) };
        }
    }

    /// <summary>Gets when the next send attempt is allowed (backoff).</summary>
    public DateTimeOffset NextAttempt => _nextAttempt;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _queue.WaitAsync(FlushInterval, stoppingToken).ConfigureAwait(false);
                    await FlushOnceAsync(force: false, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex) when (ex is not OutOfMemoryException)
                {
                    // The sender must survive anything: log and carry on after a pause.
                    _logger.LogError("IdleMon sender error ({Type}); continuing.", ex.GetType().Name);
                    await Task.Delay(FlushInterval, stoppingToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
    }

    /// <summary>On shutdown: one last flush of at most five seconds, then whatever is left goes to the spool.</summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        try
        {
            await FlushOnceAsync(force: true, cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or HttpRequestException)
        {
            _logger.LogInformation("IdleMon final flush did not finish in time.");
        }

        SpoolPending();
        _logger.LogInformation("IdleMon reporter stopped: accepted {Accepted}, rejected {Rejected}, spooled {Spooled}, dropped {Dropped}.", _status.Accepted, _status.Rejected, _status.Spooled, _status.Dropped);
        _http?.Dispose();
    }

    /// <summary>One flush: replay the spool oldest first, then send new batches. Public so tests can drive it.</summary>
    public async Task FlushOnceAsync(bool force, CancellationToken cancellationToken)
    {
        if (_dryRunFile is not null)
        {
            DrainToFile();
            return;
        }

        if (!force && _nextAttemptTimestamp != 0 && _time.GetTimestamp() < _nextAttemptTimestamp)
        {
            SpoolPending();
            return;
        }

        while (_spool?.PeekOldest() is { } oldest)
        {
            var remaining = new List<byte[]>(oldest.Envelopes);
            while (remaining.Count > 0)
            {
                var outcome = await SendEnvelopeAsync(remaining[0], cancellationToken).ConfigureAwait(false);
                if (outcome == SendOutcome.Retry)
                {
                    _spool.Rewrite(oldest.Path, remaining);
                    SpoolPending();
                    return;
                }

                remaining.RemoveAt(0);
            }

            _spool.Delete(oldest.Path);
        }

        while (_queue.Count > 0)
        {
            var batch = _queue.Take(MaxBatchMessages, IdleMonWire.MaxBatchBytes);
            if (batch.Count == 0) return;

            var envelope = BuildEnvelope(batch);
            var outcome = await SendEnvelopeAsync(envelope, cancellationToken).ConfigureAwait(false);
            if (outcome == SendOutcome.Retry)
            {
                Spool([envelope], batch.Count);
                SpoolPending();
                return;
            }
        }
    }

    private byte[] BuildEnvelope(IReadOnlyList<PendingMessage> batch) =>
        IdleMonWire.BuildEnvelope(_host, _version, _options.Instance, _time.GetUtcNow(), batch.Select(b => b.Json).ToList());

    private void DrainToFile()
    {
        while (_queue.Count > 0)
        {
            var batch = _queue.Take(MaxBatchMessages, IdleMonWire.MaxBatchBytes);
            if (batch.Count == 0) return;

            try
            {
                IdleMonSpool.AppendLine(_dryRunFile!, BuildEnvelope(batch));
                _status.AddAccepted(batch.Count);
                _status.SetLastSend(_time.GetUtcNow(), null);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                _status.AddDropped(batch.Count);
                _status.SetLastSend(_time.GetUtcNow(), "Dry run failed: " + ex.GetType().Name);
                _logger.LogWarning("IdleMon dry-run file could not be written ({Type}).", ex.GetType().Name);
            }
        }
    }

    /// <summary>Moves everything still queued into the spool (during backoff and at shutdown), so memory stays bounded and nothing is lost.</summary>
    private void SpoolPending()
    {
        if (_spool is null) return;
        while (_queue.Count > 0)
        {
            var batch = _queue.Take(MaxBatchMessages, IdleMonWire.MaxBatchBytes);
            if (batch.Count == 0) return;
            Spool([BuildEnvelope(batch)], batch.Count);
        }
    }

    private void Spool(IReadOnlyList<byte[]> envelopes, int messages)
    {
        if (_spool is null)
        {
            _status.AddDropped(messages);
            return;
        }

        try
        {
            _spool.Write(envelopes);
            _status.AddSpooled(messages);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _status.AddDropped(messages);
            _logger.LogWarning("IdleMon spool could not be written ({Type}); {Count} messages were dropped.", ex.GetType().Name, messages);
        }
    }

    private async Task<SendOutcome> SendEnvelopeAsync(byte[] envelope, CancellationToken cancellationToken, int depth = 0)
    {
        using var content = new ByteArrayContent(envelope);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        using var request = new HttpRequestMessage(HttpMethod.Post, _ingest) { Content = content };
        if (!string.IsNullOrWhiteSpace(_options.ApiKeyEnv) && Environment.GetEnvironmentVariable(_options.ApiKeyEnv) is { Length: > 0 } key)
            request.Headers.TryAddWithoutValidation("X-IdleMon-ApiKey", key);

        HttpResponseMessage response;
        try
        {
            response = await _http!.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or IOException or InvalidOperationException)
        {
            if (cancellationToken.IsCancellationRequested) return SendOutcome.Retry;

            // Connection refused, timeout, or a request the loopback guard refused: all retryable, nothing is lost (it is spooled).
            FailAttempt(null, ex is HttpRequestException ? "Connection failed" : ex is TaskCanceledException ? "Timeout" : ex.GetType().Name);
            return SendOutcome.Retry;
        }

        using (response)
        {
            var status = (int)response.StatusCode;
            switch (response.StatusCode)
            {
                case HttpStatusCode.Accepted:
                    await ReadAcceptedAsync(response, envelope, cancellationToken).ConfigureAwait(false);
                    _backoff = TimeSpan.Zero;
                    _nextAttempt = DateTimeOffset.MinValue;
                    _nextAttemptTimestamp = 0;
                    return SendOutcome.Done;

                case HttpStatusCode.BadRequest:
                    _logger.LogError("IdleMon rejected a batch as invalid (400); it is dropped. This is a bug in the reporter.");
                    _status.AddDropped(IdleMonWire.CountMessages(envelope));
                    _status.SetLastSend(_time.GetUtcNow(), "Rejected: 400 (batch dropped)");
                    return SendOutcome.Dropped;

                case HttpStatusCode.RequestEntityTooLarge:
                    return await SplitAsync(envelope, depth, cancellationToken).ConfigureAwait(false);

                case HttpStatusCode.Unauthorized:
                    if (_time.GetUtcNow() - _lastUnauthorizedLog >= TimeSpan.FromMinutes(10))
                    {
                        _lastUnauthorizedLog = _time.GetUtcNow();
                        _logger.LogWarning("IdleMon answered 401: the API key is missing or wrong. Messages are spooled; retrying slowly.");
                    }

                    FailAttempt(TimeSpan.FromSeconds(60), "401 Unauthorized (API key)");
                    return SendOutcome.Retry;

                case HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable:
                    FailAttempt(response.Headers.RetryAfter?.Delta, status + " busy");
                    return SendOutcome.Retry;

                default:
                    if (status >= 500)
                    {
                        FailAttempt(null, status + " server error");
                        return SendOutcome.Retry;
                    }

                    _logger.LogError("IdleMon answered {Status}; the batch is dropped.", status);
                    _status.AddDropped(IdleMonWire.CountMessages(envelope));
                    _status.SetLastSend(_time.GetUtcNow(), "Rejected: " + status + " (batch dropped)");
                    return SendOutcome.Dropped;
            }
        }
    }

    private async Task<SendOutcome> SplitAsync(byte[] envelope, int depth, CancellationToken cancellationToken)
    {
        var parts = depth < 12 ? IdleMonWire.Split(envelope) : null;
        if (parts is null)
        {
            _logger.LogError("IdleMon answered 413 for a single message; it is dropped.");
            _status.AddDropped(Math.Max(1, IdleMonWire.CountMessages(envelope)));
            return SendOutcome.Dropped;
        }

        var first = await SendEnvelopeAsync(parts.Value.First, cancellationToken, depth + 1).ConfigureAwait(false);
        if (first == SendOutcome.Retry) return SendOutcome.Retry; // the whole envelope is retried later; duplicates are removed by id
        return await SendEnvelopeAsync(parts.Value.Second, cancellationToken, depth + 1).ConfigureAwait(false);
    }

    private async Task ReadAcceptedAsync(HttpResponseMessage response, byte[] envelope, CancellationToken cancellationToken)
    {
        var total = IdleMonWire.CountMessages(envelope);
        var rejectedCount = 0;
        try
        {
            var body = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
            using var document = JsonDocument.Parse(body);
            if (document.RootElement.TryGetProperty("rejected", out var rejected) && rejected.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in rejected.EnumerateArray())
                {
                    rejectedCount++;
                    var id = item.TryGetProperty("id", out var i) ? i.GetString() : "?";
                    var reason = item.TryGetProperty("reason", out var r) ? r.GetString() : "?";
                    _logger.LogWarning("IdleMon rejected message {Id}: {Reason}", id, reason);
                }
            }
        }
        catch (Exception ex) when (ex is JsonException or HttpRequestException or IOException)
        {
            // An unreadable answer to a 202 is still an acceptance.
        }

        _status.AddRejected(rejectedCount);
        _status.AddAccepted(Math.Max(0, total - rejectedCount));
        _status.SetLastSend(_time.GetUtcNow(), rejectedCount == 0 ? null : $"IdleMon rejected {rejectedCount} message(s); see the log");
    }

    private void FailAttempt(TimeSpan? retryAfter, string result)
    {
        TimeSpan delay;
        if (retryAfter is { } after && after > TimeSpan.Zero)
        {
            delay = after > TimeSpan.FromMinutes(5) ? TimeSpan.FromMinutes(5) : after;
        }
        else
        {
            _backoff = _backoff == TimeSpan.Zero ? TimeSpan.FromSeconds(1) : TimeSpan.FromTicks(Math.Min(_backoff.Ticks * 2, MaxBackoff.Ticks));
            delay = _backoff;
        }

        if (result.StartsWith("401", StringComparison.Ordinal) && delay < TimeSpan.FromSeconds(60)) delay = TimeSpan.FromSeconds(60);

        var jitter = 1 + ((_random.NextDouble() - 0.5) * 0.4);
        _nextAttempt = _time.GetUtcNow() + (delay * jitter);
        _nextAttemptTimestamp = Math.Max(1, _time.GetTimestamp() + (long)(delay.TotalSeconds * jitter * _time.TimestampFrequency)); // monotonic: a clock step cannot freeze or skip the backoff
        _status.SetLastSend(_time.GetUtcNow(), $"{result}; retry in {delay.TotalSeconds:0} s");
    }
}
