using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace S3CompareService.Core.Models;

public class S3EndpointPreset
{
    public string Name { get; set; } = string.Empty;
    public string Url { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;

    public override string ToString() => Name;
}

public class S3BucketConfig : INotifyPropertyChanged
{
    private string _endpointUrl = "https://s3.wasabisys.com";
    private string _region = "us-east-1";
    private string _bucketName = string.Empty;
    private string _accessKey = string.Empty;
    private string _secretKey = string.Empty;
    private string _prefix = string.Empty;

    public string EndpointUrl
    {
        get => _endpointUrl;
        set { if (_endpointUrl != value) { _endpointUrl = value; OnPropertyChanged(); } }
    }

    public string Region
    {
        get => _region;
        set { if (_region != value) { _region = value; OnPropertyChanged(); } }
    }

    public string BucketName
    {
        get => _bucketName;
        set { if (_bucketName != value) { _bucketName = value; OnPropertyChanged(); } }
    }

    public string AccessKey
    {
        get => _accessKey;
        set { if (_accessKey != value) { _accessKey = value; OnPropertyChanged(); } }
    }

    public string SecretKey
    {
        get => _secretKey;
        set { if (_secretKey != value) { _secretKey = value; OnPropertyChanged(); } }
    }

    public string Prefix
    {
        get => _prefix;
        set { if (_prefix != value) { _prefix = value; OnPropertyChanged(); } }
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    protected void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
}

public class S3ObjectSummary
{
    public string Key { get; set; } = string.Empty;
    public long Size { get; set; }
    public string ETag { get; set; } = string.Empty;
    public DateTime LastModified { get; set; }
    public string StorageClass { get; set; } = string.Empty;

    public string FormattedSize => FormatSize(Size);

    public static string FormatSize(long bytes)
    {
        string[] suffixes = { "B", "KB", "MB", "GB", "TB", "PB" };
        int counter = 0;
        decimal number = bytes;
        while (Math.Round(number / 1024) >= 1 && counter < suffixes.Length - 1)
        {
            number /= 1024;
            counter++;
        }
        return $"{number:n2} {suffixes[counter]}";
    }
}
