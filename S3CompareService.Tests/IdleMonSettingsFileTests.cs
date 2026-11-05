using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using S3CompareService.Worker;

namespace S3CompareService.Tests;

[TestClass]
[DoNotParallelize] // the API tests point the process-wide DatabaseConfig at their own folder
public class IdleMonSettingsFileTests
{
    private string _dir = string.Empty;

    [TestInitialize]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"s3local_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    [TestCleanup]
    public void Cleanup()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, true); } catch { }
    }

    [TestMethod]
    public void PathFor_PutsTheFileInTheDataDirectory()
    {
        Assert.AreEqual(Path.Combine(_dir, "appsettings.Local.json"), LocalSettings.PathFor("\"" + _dir + "\""));
    }

    [TestMethod]
    public void Apply_OverridesTheBaseFile_AndTheCommandLineStillWins()
    {
        var baseFile = Path.Combine(_dir, "base.json");
        File.WriteAllText(baseFile, """{ "IdleMon": { "Enabled": false, "Instance": "base", "AgentUrl": "http://127.0.0.1:1" } }""");
        var local = LocalSettings.PathFor(_dir);
        File.WriteAllText(local, """{ "IdleMon": { "Enabled": true, "Instance": "local", "HeartbeatIntervalSeconds": 30 } }""");

        string[] args = ["--IdleMon:Instance=cmdline"];
        var builder = new ConfigurationBuilder().AddJsonFile(baseFile).AddCommandLine(args);
        LocalSettings.Apply(builder, local, args);
        var config = builder.Build();

        Assert.AreEqual("True", config["IdleMon:Enabled"], "local file overrides appsettings.json");
        Assert.AreEqual("30", config["IdleMon:HeartbeatIntervalSeconds"]);
        Assert.AreEqual("cmdline", config["IdleMon:Instance"], "the command line wins over the local file");
        Assert.AreEqual("http://127.0.0.1:1", config["IdleMon:AgentUrl"], "keys the local file does not set stay as they were");
    }

    [TestMethod]
    public void Apply_MissingFile_IsIgnored()
    {
        var builder = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["IdleMon:Instance"] = "x" });
        LocalSettings.Apply(builder, LocalSettings.PathFor(_dir), []);
        Assert.AreEqual("x", builder.Build()["IdleMon:Instance"]);
    }

    private WebApplicationFactory<Program> Factory() => new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
    {
        b.UseSetting("S3Compare:DataDirectory", _dir);
        b.UseSetting("S3Compare:Urls", "http://localhost:5088");
    });

    [TestMethod]
    public async Task Status_WithoutLocalFile_ReportsThePathAndThatItIsMissing_AlsoWhenDisabled()
    {
        await using var factory = Factory();
        using var client = factory.CreateClient();
        client.DefaultRequestHeaders.Add("X-S3Compare-Client", "ui");
        var resp = await client.GetAsync("/api/idlemon/status");
        Assert.AreEqual(HttpStatusCode.OK, resp.StatusCode);
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        Assert.IsFalse(doc.RootElement.GetProperty("enabled").GetBoolean());
        Assert.AreEqual(LocalSettings.PathFor(_dir), doc.RootElement.GetProperty("settingsFile").GetString());
        Assert.IsFalse(doc.RootElement.GetProperty("settingsFileExists").GetBoolean());
        Assert.AreEqual(JsonValueKind.False, doc.RootElement.GetProperty("apiKeyPresent").ValueKind);
    }

    [TestMethod]
    public async Task Status_LocalFileInDataDirectory_EnablesIdleMon_AndApiKeyPresenceNeverLeaksTheKey()
    {
        var envName = "S3C_TEST_KEY_" + Guid.NewGuid().ToString("N");
        const string secret = "super-secret-key-value-123";
        var dry = Path.Combine(_dir, "dry.jsonl");
        File.WriteAllText(LocalSettings.PathFor(_dir),
            JsonSerializer.Serialize(new { IdleMon = new { Enabled = true, DryRunFile = dry, ApiKeyEnv = envName } }));
        Environment.SetEnvironmentVariable(envName, secret);
        try
        {
            await using var factory = Factory();
            using var client = factory.CreateClient();
            client.DefaultRequestHeaders.Add("X-S3Compare-Client", "ui");
            var body = await client.GetStringAsync("/api/idlemon/status");
            Assert.IsFalse(body.Contains(secret, StringComparison.Ordinal), "the key must never appear in the status");
            using var doc = JsonDocument.Parse(body);
            Assert.IsTrue(doc.RootElement.GetProperty("enabled").GetBoolean());
            Assert.AreEqual("DryRun", doc.RootElement.GetProperty("mode").GetString());
            Assert.IsTrue(doc.RootElement.GetProperty("settingsFileExists").GetBoolean());
            Assert.IsTrue(doc.RootElement.GetProperty("apiKeyPresent").GetBoolean());
            Assert.AreEqual(Path.GetFullPath(dry), doc.RootElement.GetProperty("dryRunFile").GetString());

            Environment.SetEnvironmentVariable(envName, "");
            using var doc2 = JsonDocument.Parse(await client.GetStringAsync("/api/idlemon/status"));
            Assert.IsFalse(doc2.RootElement.GetProperty("apiKeyPresent").GetBoolean(), "an empty variable counts as not set");
        }
        finally
        {
            Environment.SetEnvironmentVariable(envName, null);
        }
    }
}
