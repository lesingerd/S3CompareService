using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

public static class S3ClientFactory
{
    public static List<S3EndpointPreset> GetWasabiEndpoints()
    {
        return new List<S3EndpointPreset>
        {
            new() { Name = "Wasabi US East 1 (N. Virginia)", Url = "https://s3.wasabisys.com", Region = "us-east-1" },
            new() { Name = "Wasabi US East 2 (N. Virginia)", Url = "https://s3.us-east-2.wasabisys.com", Region = "us-east-2" },
            new() { Name = "Wasabi US Central 1 (Texas)", Url = "https://s3.us-central-1.wasabisys.com", Region = "us-central-1" },
            new() { Name = "Wasabi US West 1 (Oregon)", Url = "https://s3.us-west-1.wasabisys.com", Region = "us-west-1" },
            new() { Name = "Wasabi EU Central 1 (Amsterdam)", Url = "https://s3.eu-central-1.wasabisys.com", Region = "eu-central-1" },
            new() { Name = "Wasabi EU Central 2 (Frankfurt)", Url = "https://s3.eu-central-2.wasabisys.com", Region = "eu-central-2" },
            new() { Name = "Wasabi EU West 1 (London)", Url = "https://s3.eu-west-1.wasabisys.com", Region = "eu-west-1" },
            new() { Name = "Wasabi EU West 2 (Paris)", Url = "https://s3.eu-west-2.wasabisys.com", Region = "eu-west-2" },
            new() { Name = "Wasabi EU South 1 (Milan)", Url = "https://s3.eu-south-1.wasabisys.com", Region = "eu-south-1" },
            new() { Name = "Wasabi AP Northeast 1 (Tokyo)", Url = "https://s3.ap-northeast-1.wasabisys.com", Region = "ap-northeast-1" },
            new() { Name = "Wasabi AP Northeast 2 (Osaka)", Url = "https://s3.ap-northeast-2.wasabisys.com", Region = "ap-northeast-2" },
            new() { Name = "Wasabi AP Southeast 1 (Singapore)", Url = "https://s3.ap-southeast-1.wasabisys.com", Region = "ap-southeast-1" },
            new() { Name = "Wasabi AP Southeast 2 (Sydney)", Url = "https://s3.ap-southeast-2.wasabisys.com", Region = "ap-southeast-2" },
            new() { Name = "Wasabi CA Central 1 (Toronto)", Url = "https://s3.ca-central-1.wasabisys.com", Region = "ca-central-1" },
            new() { Name = "AWS S3 Standard (us-east-1)", Url = "https://s3.amazonaws.com", Region = "us-east-1" },
            new() { Name = "Custom Endpoint...", Url = "", Region = "us-east-1" }
        };
    }

    public static AmazonS3Client CreateClient(S3BucketConfig config)
    {
        var credentials = new BasicAWSCredentials(config.AccessKey.Trim(), config.SecretKey.Trim());
        
        var s3Config = new AmazonS3Config
        {
            ForcePathStyle = true,
            Timeout = TimeSpan.FromSeconds(45),
            MaxErrorRetry = 3
        };

        if (!string.IsNullOrWhiteSpace(config.EndpointUrl))
        {
            var url = config.EndpointUrl.Trim();
            if (!url.StartsWith("http://", StringComparison.OrdinalIgnoreCase) && 
                !url.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                url = "https://" + url;
            }
            s3Config.ServiceURL = url;
        }

        if (!string.IsNullOrWhiteSpace(config.Region))
        {
            s3Config.AuthenticationRegion = config.Region.Trim();
        }

        return new AmazonS3Client(credentials, s3Config);
    }

    public static async Task<(bool Success, string Message)> TestConnectionAsync(
        S3BucketConfig config, 
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(config.BucketName))
            return (false, "Bucket name is required.");

        if (string.IsNullOrWhiteSpace(config.AccessKey))
            return (false, "Access Key ID is required.");

        if (string.IsNullOrWhiteSpace(config.SecretKey))
            return (false, "Secret Access Key is required.");

        try
        {
            using var client = CreateClient(config);
            var request = new ListObjectsV2Request
            {
                BucketName = config.BucketName.Trim(),
                MaxKeys = 1,
                Prefix = string.IsNullOrWhiteSpace(config.Prefix) ? null : config.Prefix.Trim()
            };

            var response = await client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
            return (true, $"Connection successful! Found {response.KeyCount} object(s) with prefix check.");
        }
        catch (AmazonS3Exception s3Ex)
        {
            return (false, $"S3 Error [{s3Ex.ErrorCode}]: {s3Ex.Message}");
        }
        catch (Exception ex)
        {
            return (false, $"Connection Error: {ex.Message}");
        }
    }

    public static async Task<(bool Success, List<string> Buckets, string Message)> ListBucketsAsync(
        S3BucketConfig config,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(config.AccessKey))
            return (false, new List<string>(), "Access Key ID is required.");

        if (string.IsNullOrWhiteSpace(config.SecretKey))
            return (false, new List<string>(), "Secret Access Key is required.");

        try
        {
            using var client = CreateClient(config);
            var response = await client.ListBucketsAsync(cancellationToken).ConfigureAwait(false);
            var buckets = response.Buckets.Select(b => b.BucketName).OrderBy(b => b, StringComparer.OrdinalIgnoreCase).ToList();
            return (true, buckets, $"Discovered {buckets.Count} bucket(s).");
        }
        catch (AmazonS3Exception s3Ex)
        {
            return (false, new List<string>(), $"S3 Error [{s3Ex.ErrorCode}]: {s3Ex.Message}");
        }
        catch (Exception ex)
        {
            return (false, new List<string>(), $"Discovery Error: {ex.Message}");
        }
    }
}
