using System.Text.Json;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;

namespace S3CompareService.Tests;

[TestClass]
public class ReportExporterTests
{
    [TestMethod]
    public async Task ExportToCsv_OutputsCompleteMetadata_AndProperGracePeriodTagging()
    {
        var run = new ScanRun
        {
            Id = "run-abcdef123456",
            JobName = "Daily S3 Sync",
            Status = RunStatus.Completed,
            Mode = ComparisonMode.ListingOnly,
            ObjectsCompared = 1000,
            MatchedCount = 998,
            TotalDifferences = 2,
            FilteredDifferencesCount = 1,
            InFlightDifferencesCount = 1,
            ReplicationGracePeriodMinutes = 15,
            IsScanWindowClipped = true,
            ScanWindowNotes = "Objects modified after cutoff flagged as in-flight replication."
        };

        var diffs = new List<RecordedDifference>
        {
            new()
            {
                Id = 1,
                ScanRunId = run.Id,
                Key = "data/in_flight.parquet",
                Type = DifferenceType.MissingInBucketB,
                Details = "In-flight replication item",
                IsWithinGracePeriod = true,
                BucketASize = 50000,
                BucketALastModified = DateTime.UtcNow.AddMinutes(-5)
            },
            new()
            {
                Id = 2,
                ScanRunId = run.Id,
                Key = "data/stale_missing.parquet",
                Type = DifferenceType.MissingInBucketB,
                Details = "Confirmed disparity",
                IsWithinGracePeriod = false,
                BucketASize = 60000,
                BucketALastModified = DateTime.UtcNow.AddHours(-10)
            }
        };

        var writer = new StringWriter();
        await ReportExporter.ExportToCsvAsync(writer, run, diffs, includeGracePeriodItems: true);
        string csv = writer.ToString();

        Assert.IsTrue(csv.Contains("# Run ID                  : run-abcdef123456"));
        Assert.IsTrue(csv.Contains("# Confirmed Disparities   : 1"));
        Assert.IsTrue(csv.Contains("# In-Flight Replication   : 1"));
        Assert.IsTrue(csv.Contains("# Scan Window Clipped     : True"));
        Assert.IsTrue(csv.Contains("data/in_flight.parquet"));
        Assert.IsTrue(csv.Contains("IN_FLIGHT_REPLICATION"));
        Assert.IsTrue(csv.Contains("data/stale_missing.parquet"));
        Assert.IsTrue(csv.Contains("CONFIRMED_DISPARITY"));
    }

    [TestMethod]
    public async Task ExportToJson_SerializesCompleteReportTree()
    {
        var run = new ScanRun
        {
            Id = "run-test-json",
            JobName = "Test JSON",
            Status = RunStatus.Completed
        };

        var diffs = new List<RecordedDifference>
        {
            new() { Id = 1, ScanRunId = run.Id, Key = "item1.bin", Type = DifferenceType.SizeMismatch }
        };

        var mem = new MemoryStream();
        await ReportExporter.ExportToJsonAsync(mem, run, diffs, includeGracePeriodItems: true);
        mem.Position = 0;

        using var doc = await JsonDocument.ParseAsync(mem);
        var root = doc.RootElement;
        Assert.IsTrue(root.TryGetProperty("run", out var runProp));
        Assert.AreEqual("run-test-json", runProp.GetProperty("id").GetString());
        Assert.IsTrue(root.TryGetProperty("differences", out var diffsProp));
        Assert.AreEqual(1, diffsProp.GetArrayLength());
    }
}
