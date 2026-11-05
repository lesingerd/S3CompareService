namespace S3CompareService.Worker.IdleMon;

/// <summary>
/// The reporter talks to the IdleMon agent on this machine and nothing else. This handler is the last line of defence: it
/// refuses every request whose host is not loopback, and every request that is not <c>POST {AgentUrl}/api/v1/ingest</c>. A
/// refused request never reaches the network.
/// </summary>
public sealed class LoopbackOnlyHandler : DelegatingHandler
{
    /// <summary>Path of the ingest endpoint.</summary>
    public const string IngestPath = "/api/v1/ingest";

    private readonly string _basePath;

    /// <summary>Creates the handler around the real network handler.</summary>
    public LoopbackOnlyHandler(HttpMessageHandler inner, Uri agentUrl)
        : base(inner)
    {
        _basePath = agentUrl.AbsolutePath.TrimEnd('/');
    }

    /// <inheritdoc />
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var uri = request.RequestUri ?? throw new InvalidOperationException("The IdleMon reporter refuses a request without an address.");
        if (!uri.IsAbsoluteUri || uri.Scheme is not ("http" or "https"))
            throw new InvalidOperationException("The IdleMon reporter only sends http or https requests.");
        if (!IdleMonOptions.IsLoopbackHost(uri.Host))
            throw new InvalidOperationException("The IdleMon reporter only talks to this machine; the request was refused (host is not loopback).");
        if (request.Method != HttpMethod.Post || uri.AbsolutePath != _basePath + IngestPath)
            throw new InvalidOperationException("The IdleMon reporter may only POST /api/v1/ingest; the request was refused.");
        return base.SendAsync(request, cancellationToken);
    }
}
