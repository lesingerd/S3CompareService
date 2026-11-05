namespace S3CompareService.Worker;

/// <summary>
/// The optional local override file <c>appsettings.Local.json</c>. It lives in the data directory, not in the install folder,
/// so a service update (which overwrites the install folder) never touches it. It is read once at start-up.
/// </summary>
public static class LocalSettings
{
    /// <summary>The file name inside the data directory.</summary>
    public const string FileName = "appsettings.Local.json";

    /// <summary>Gets the full path of the override file for a data directory.</summary>
    public static string PathFor(string dataDirectory) =>
        Path.Combine(Path.GetFullPath(dataDirectory.Trim().Trim('"')), FileName);

    /// <summary>
    /// Adds the override file (if it exists) after appsettings.json, then puts environment variables and the command line back on
    /// top, so that command-line arguments still win. Changes need a service restart (<c>reloadOnChange</c> is off).
    /// </summary>
    public static void Apply(IConfigurationBuilder configuration, string path, string[] args)
    {
        configuration.AddJsonFile(path, optional: true, reloadOnChange: false);
        configuration.AddEnvironmentVariables();
        configuration.AddCommandLine(args);
    }
}
