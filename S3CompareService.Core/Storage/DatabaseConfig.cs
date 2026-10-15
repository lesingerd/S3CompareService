namespace S3CompareService.Core.Storage;

public static class DatabaseConfig
{
    private static string? _customDbPath;

    public static string GetDefaultDatabasePath()
    {
        if (!string.IsNullOrWhiteSpace(_customDbPath))
            return _customDbPath;

        // Default path: C:\ProgramData\S3CompareService\s3compare.db
        // Fallback: AppDomain BaseDirectory if ProgramData is inaccessible
        string programData = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
        string dir = string.IsNullOrWhiteSpace(programData)
            ? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data")
            : Path.Combine(programData, "S3CompareService");

        if (!Directory.Exists(dir))
        {
            try
            {
                Directory.CreateDirectory(dir);
            }
            catch
            {
                dir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data");
                if (!Directory.Exists(dir))
                {
                    Directory.CreateDirectory(dir);
                }
            }
        }

        return Path.Combine(dir, "s3compareservice.db");
    }

    public static void SetCustomDatabasePath(string path)
    {
        _customDbPath = path;
        string? dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dir) && !Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }
    }

    public static string GetConnectionString(string? dbPath = null)
    {
        string path = dbPath ?? GetDefaultDatabasePath();
        return $"Data Source={path};Mode=ReadWriteCreate;Cache=Shared;";
    }
}
