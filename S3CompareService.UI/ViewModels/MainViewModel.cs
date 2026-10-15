using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Windows;
using System.Windows.Threading;
using Microsoft.Win32;
using S3CompareService.Core.Engine;
using S3CompareService.Core.Models;
using S3CompareService.Core.Storage;
using S3CompareService.UI.Common;
using S3CompareService.UI.Services;

namespace S3CompareService.UI.ViewModels;

public class MainViewModel : ViewModelBase
{
    private readonly ServiceClient _client;
    private readonly DispatcherTimer _timer;

    // Service Status
    private ServiceStatusDto _serviceStatus = new();
    public ServiceStatusDto ServiceStatus
    {
        get => _serviceStatus;
        set => SetField(ref _serviceStatus, value);
    }

    private ActiveScanDto? _currentActiveScan;
    public ActiveScanDto? CurrentActiveScan
    {
        get => _currentActiveScan;
        set => SetField(ref _currentActiveScan, value);
    }

    private string _statusMessage = "Ready";
    public string StatusMessage
    {
        get => _statusMessage;
        set => SetField(ref _statusMessage, value);
    }

    // Jobs
    public ObservableCollection<ComparisonJob> Jobs { get; } = new();

    private ComparisonJob? _selectedJob;
    public ComparisonJob? SelectedJob
    {
        get => _selectedJob;
        set
        {
            if (value != null && value.AlertConfig == null)
            {
                value.AlertConfig = new JobAlertConfig();
            }
            if (SetField(ref _selectedJob, value))
            {
                OnPropertyChanged(nameof(HasSelectedJob));
                SyncPresetSelection();
            }
        }
    }
    public bool HasSelectedJob => SelectedJob != null;

    // Presets
    public ObservableCollection<S3EndpointPreset> PresetsA { get; } = new();
    public ObservableCollection<S3EndpointPreset> PresetsB { get; } = new();

    private S3EndpointPreset? _selectedPresetA;
    public S3EndpointPreset? SelectedPresetA
    {
        get => _selectedPresetA;
        set
        {
            if (SetField(ref _selectedPresetA, value) && value != null && SelectedJob != null)
            {
                if (!string.IsNullOrWhiteSpace(value.Url))
                {
                    SelectedJob.BucketA.EndpointUrl = value.Url;
                    SelectedJob.BucketA.Region = value.Region;
                    OnPropertyChanged(nameof(SelectedJob));
                }
            }
        }
    }

    private S3EndpointPreset? _selectedPresetB;
    public S3EndpointPreset? SelectedPresetB
    {
        get => _selectedPresetB;
        set
        {
            if (SetField(ref _selectedPresetB, value) && value != null && SelectedJob != null)
            {
                if (!string.IsNullOrWhiteSpace(value.Url))
                {
                    SelectedJob.BucketB.EndpointUrl = value.Url;
                    SelectedJob.BucketB.Region = value.Region;
                    OnPropertyChanged(nameof(SelectedJob));
                }
            }
        }
    }

    // Runs History
    public ObservableCollection<ScanRun> Runs { get; } = new();

    private ScanRun? _selectedRun;
    public ScanRun? SelectedRun
    {
        get => _selectedRun;
        set
        {
            if (SetField(ref _selectedRun, value))
            {
                OnPropertyChanged(nameof(HasSelectedRun));
                _ = LoadRunDetailsAsync(value);
            }
        }
    }
    public bool HasSelectedRun => SelectedRun != null;

    public ObservableCollection<BucketMetadataSnapshot> SelectedRunSnapshots { get; } = new();

    // Differences Explorer
    public ObservableCollection<RecordedDifference> Differences { get; } = new();

    private RecordedDifference? _selectedDifference;
    public RecordedDifference? SelectedDifference
    {
        get => _selectedDifference;
        set => SetField(ref _selectedDifference, value);
    }

    private DifferenceType? _filterType = null;
    public DifferenceType? FilterType
    {
        get => _filterType;
        set
        {
            if (SetField(ref _filterType, value))
            {
                _diffPageIndex = 0;
                _ = LoadDifferencesAsync();
            }
        }
    }

    private int _gracePeriodFilterMode = 0; // 0 = Confirmed Disparities Only, 1 = All Raw Differences, 2 = In-Flight Only
    public int GracePeriodFilterMode
    {
        get => _gracePeriodFilterMode;
        set
        {
            if (SetField(ref _gracePeriodFilterMode, value))
            {
                _diffPageIndex = 0;
                _ = LoadDifferencesAsync();
            }
        }
    }

    private string _filterSearchKey = string.Empty;
    public string FilterSearchKey
    {
        get => _filterSearchKey;
        set
        {
            if (SetField(ref _filterSearchKey, value))
            {
                _diffPageIndex = 0;
                _ = LoadDifferencesAsync();
            }
        }
    }

    private int _diffPageIndex = 0;
    private int _diffPageSize = 50;
    private int _diffTotalCount = 0;

    public string PaginationDisplay => _diffTotalCount == 0 ? "0 / 0" : $"{_diffPageIndex * _diffPageSize + 1} - {Math.Min((_diffPageIndex + 1) * _diffPageSize, _diffTotalCount)} of {_diffTotalCount:N0}";

    // Audit Logs
    public ObservableCollection<ServiceAuditLog> AuditLogs { get; } = new();

    // Commands
    public RelayCommand RefreshStatusCommand { get; }
    public RelayCommand StartServiceCommand { get; }
    public RelayCommand StopServiceCommand { get; }
    public RelayCommand RestartServiceCommand { get; }

    public RelayCommand PauseActiveScanCommand { get; }
    public RelayCommand ResumeActiveScanCommand { get; }
    public RelayCommand CancelActiveScanCommand { get; }

    public RelayCommand NewJobCommand { get; }
    public RelayCommand SaveJobCommand { get; }
    public RelayCommand DeleteJobCommand { get; }
    public RelayCommand RunJobNowCommand { get; }
    public RelayCommand TestConnectionACommand { get; }
    public RelayCommand TestConnectionBCommand { get; }
    public RelayCommand TestSmtpCommand { get; }
    public RelayCommand TestAzureWebhookCommand { get; }

    public RelayCommand RefreshRunsCommand { get; }
    public RelayCommand RefreshDifferencesCommand { get; }
    public RelayCommand NextDiffPageCommand { get; }
    public RelayCommand PrevDiffPageCommand { get; }

    public RelayCommand ExportCsvCommand { get; }
    public RelayCommand ExportJsonCommand { get; }
    public RelayCommand RefreshLogsCommand { get; }

    public MainViewModel()
    {
        _client = new ServiceClient();

        RefreshStatusCommand = new RelayCommand(async () => await RefreshStatusAsync());
        StartServiceCommand = new RelayCommand(() => RunServiceControl("start"));
        StopServiceCommand = new RelayCommand(() => RunServiceControl("stop"));
        RestartServiceCommand = new RelayCommand(() =>
        {
            RunServiceControl("stop");
            Thread.Sleep(1500);
            RunServiceControl("start");
        });

        PauseActiveScanCommand = new RelayCommand(async () =>
        {
            if (CurrentActiveScan != null) await _client.PauseRunAsync(CurrentActiveScan.ScanRunId);
        });
        ResumeActiveScanCommand = new RelayCommand(async () =>
        {
            if (CurrentActiveScan != null) await _client.ResumeRunAsync(CurrentActiveScan.ScanRunId);
        });
        CancelActiveScanCommand = new RelayCommand(async () =>
        {
            if (CurrentActiveScan != null) await _client.CancelRunAsync(CurrentActiveScan.ScanRunId);
        });

        NewJobCommand = new RelayCommand(CreateNewJob);
        SaveJobCommand = new RelayCommand(async () => await SaveJobAsync());
        DeleteJobCommand = new RelayCommand(async () => await DeleteJobAsync());
        RunJobNowCommand = new RelayCommand(async () => await TriggerSelectedJobAsync());

        TestConnectionACommand = new RelayCommand(async () => await TestConnectionAsync(true));
        TestConnectionBCommand = new RelayCommand(async () => await TestConnectionAsync(false));
        TestSmtpCommand = new RelayCommand(async () => await TestSmtpAsync());
        TestAzureWebhookCommand = new RelayCommand(async () => await TestAzureWebhookAsync());

        RefreshRunsCommand = new RelayCommand(async () => await LoadRunsAsync());
        RefreshDifferencesCommand = new RelayCommand(async () => await LoadDifferencesAsync());
        NextDiffPageCommand = new RelayCommand(async () =>
        {
            if ((_diffPageIndex + 1) * _diffPageSize < _diffTotalCount)
            {
                _diffPageIndex++;
                await LoadDifferencesAsync();
            }
        });
        PrevDiffPageCommand = new RelayCommand(async () =>
        {
            if (_diffPageIndex > 0)
            {
                _diffPageIndex--;
                await LoadDifferencesAsync();
            }
        });

        ExportCsvCommand = new RelayCommand(async () => await ExportReportAsync(false));
        ExportJsonCommand = new RelayCommand(async () => await ExportReportAsync(true));
        RefreshLogsCommand = new RelayCommand(async () => await LoadLogsAsync());

        // Initialize presets
        var presets = S3ClientFactory.GetWasabiEndpoints();
        foreach (var p in presets)
        {
            PresetsA.Add(p);
            PresetsB.Add(p);
        }

        // Timer for periodic polling
        _timer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(1.5) };
        _timer.Tick += async (_, _) => await OnTimerTickAsync();
        _timer.Start();

        _ = InitializeAsync();
    }

    private async Task InitializeAsync()
    {
        await RefreshStatusAsync();
        await LoadJobsAsync();
        await LoadRunsAsync();
        await LoadLogsAsync();
    }

    private async Task OnTimerTickAsync()
    {
        await RefreshStatusAsync();
        if (CurrentActiveScan != null)
        {
            // If viewing differences of the currently active scan, auto-refresh
            if (SelectedRun != null && SelectedRun.Id == CurrentActiveScan.ScanRunId)
            {
                await LoadDifferencesAsync();
            }
        }
    }

    public async Task RefreshStatusAsync()
    {
        var status = await _client.GetStatusAsync();
        ServiceStatus = status;
        CurrentActiveScan = status.ActiveScans.FirstOrDefault();
    }

    public async Task LoadJobsAsync()
    {
        var jobs = await _client.GetJobsAsync();
        Jobs.Clear();
        foreach (var j in jobs) Jobs.Add(j);

        if (SelectedJob == null && Jobs.Count > 0)
        {
            SelectedJob = Jobs[0];
        }
    }

    private void CreateNewJob()
    {
        var job = new ComparisonJob
        {
            Name = $"Compare Job {Jobs.Count + 1}",
            ScheduleType = JobScheduleType.IntervalMinutes,
            IntervalMinutes = 60,
            ReplicationGracePeriodMinutes = 15,
            DetectSequenceGaps = true,
            Mode = ComparisonMode.ListingOnly
        };
        job.BucketA.EndpointUrl = "https://s3.wasabisys.com";
        job.BucketB.EndpointUrl = "https://s3.wasabisys.com";
        Jobs.Insert(0, job);
        SelectedJob = job;
    }

    public async Task SaveJobAsync()
    {
        if (SelectedJob == null) return;
        try
        {
            await _client.SaveJobAsync(SelectedJob);
            StatusMessage = $"Saved job '{SelectedJob.Name}'.";
            await LoadJobsAsync();
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save job: {ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    public async Task DeleteJobAsync()
    {
        if (SelectedJob == null) return;
        var res = MessageBox.Show($"Are you sure you want to delete job '{SelectedJob.Name}'?", "Confirm Delete", MessageBoxButton.YesNo, MessageBoxImage.Warning);
        if (res == MessageBoxResult.Yes)
        {
            await _client.DeleteJobAsync(SelectedJob.Id);
            StatusMessage = $"Deleted job '{SelectedJob.Name}'.";
            await LoadJobsAsync();
        }
    }

    public async Task TriggerSelectedJobAsync()
    {
        if (SelectedJob == null) return;
        var (success, msg) = await _client.TriggerJobAsync(SelectedJob.Id);
        if (success)
        {
            StatusMessage = $"Triggered scan for '{SelectedJob.Name}'.";
            await RefreshStatusAsync();
        }
        else
        {
            MessageBox.Show(msg, "Trigger Scan", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    public async Task TestConnectionAsync(bool isBucketA)
    {
        if (SelectedJob == null) return;
        var config = isBucketA ? SelectedJob.BucketA : SelectedJob.BucketB;
        string sideName = isBucketA ? "Bucket A" : "Bucket B";

        StatusMessage = $"Testing connection to {sideName} ({config.BucketName})...";
        var (ok, msg) = await _client.TestConnectionAsync(config);

        if (ok)
        {
            MessageBox.Show($"[SUCCESS] {sideName} connection verified:\n{msg}", $"{sideName} Test", MessageBoxButton.OK, MessageBoxImage.Information);
            StatusMessage = $"{sideName} connection successful.";
        }
        else
        {
            MessageBox.Show($"[FAILED] {sideName} connection failed:\n{msg}", $"{sideName} Test", MessageBoxButton.OK, MessageBoxImage.Error);
            StatusMessage = $"{sideName} test failed.";
        }
    }

    public async Task TestSmtpAsync()
    {
        if (SelectedJob == null) return;
        StatusMessage = "Sending test alert via SMTP...";
        var (ok, msg) = await _client.TestSmtpAsync(SelectedJob.AlertConfig);
        if (ok)
        {
            MessageBox.Show($"[SUCCESS] SMTP alert test succeeded:\n{msg}", "SMTP Alert Test", MessageBoxButton.OK, MessageBoxImage.Information);
            StatusMessage = "SMTP alert test successful.";
        }
        else
        {
            MessageBox.Show($"[FAILED] SMTP alert test failed:\n{msg}", "SMTP Alert Test", MessageBoxButton.OK, MessageBoxImage.Error);
            StatusMessage = "SMTP alert test failed.";
        }
    }

    public async Task TestAzureWebhookAsync()
    {
        if (SelectedJob == null) return;
        StatusMessage = "Sending test payload to Azure webhook...";
        var (ok, msg) = await _client.TestAzureWebhookAsync(SelectedJob.AlertConfig);
        if (ok)
        {
            MessageBox.Show($"[SUCCESS] Azure alert test succeeded:\n{msg}", "Azure Webhook Test", MessageBoxButton.OK, MessageBoxImage.Information);
            StatusMessage = "Azure webhook test successful.";
        }
        else
        {
            MessageBox.Show($"[FAILED] Azure alert test failed:\n{msg}", "Azure Webhook Test", MessageBoxButton.OK, MessageBoxImage.Error);
            StatusMessage = "Azure webhook test failed.";
        }
    }

    public async Task LoadRunsAsync()
    {
        var runs = await _client.GetRunsAsync();
        Runs.Clear();
        foreach (var r in runs) Runs.Add(r);

        if (SelectedRun == null && Runs.Count > 0)
        {
            SelectedRun = Runs[0];
        }
    }

    private async Task LoadRunDetailsAsync(ScanRun? run)
    {
        SelectedRunSnapshots.Clear();
        if (run == null)
        {
            Differences.Clear();
            return;
        }

        var snapshots = await _client.GetSnapshotsAsync(run.Id);
        foreach (var s in snapshots) SelectedRunSnapshots.Add(s);

        _diffPageIndex = 0;
        await LoadDifferencesAsync();
    }

    public async Task LoadDifferencesAsync()
    {
        if (SelectedRun == null)
        {
            Differences.Clear();
            return;
        }

        bool? withinGrace = GracePeriodFilterMode switch
        {
            0 => false, // Confirmed disparities only (exclude grace period)
            1 => null,  // All raw records
            2 => true,  // In-flight replication lag items only
            _ => null
        };

        var page = await _client.GetDifferencesAsync(
            SelectedRun.Id,
            FilterType,
            withinGrace,
            FilterSearchKey,
            _diffPageIndex * _diffPageSize,
            _diffPageSize);

        _diffTotalCount = page.TotalCount;
        Differences.Clear();
        foreach (var item in page.Items) Differences.Add(item);

        OnPropertyChanged(nameof(PaginationDisplay));
    }

    public async Task LoadLogsAsync()
    {
        var logs = await _client.GetLogsAsync();
        AuditLogs.Clear();
        foreach (var l in logs) AuditLogs.Add(l);
    }

    private async Task ExportReportAsync(bool isJson)
    {
        if (SelectedRun == null)
        {
            MessageBox.Show("Please select a scan run first to export its results.", "Export", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var dlg = new SaveFileDialog
        {
            FileName = isJson
                ? $"s3compare_audit_{SelectedRun.JobName}_{SelectedRun.Id[..8]}.json"
                : $"s3compare_audit_{SelectedRun.JobName}_{SelectedRun.Id[..8]}.csv",
            Filter = isJson ? "JSON Files (*.json)|*.json" : "CSV Files (*.csv)|*.csv"
        };

        if (dlg.ShowDialog() == true)
        {
            bool includeGrace = GracePeriodFilterMode != 0;
            var diffs = await _client.GetDifferencesAsync(SelectedRun.Id, null, includeGrace ? null : false, null, 0, 100000);

            if (isJson)
            {
                await using var fs = File.Create(dlg.FileName);
                await ReportExporter.ExportToJsonAsync(fs, SelectedRun, diffs.Items, SelectedRunSnapshots, includeGrace);
            }
            else
            {
                await using var writer = new StreamWriter(dlg.FileName);
                await ReportExporter.ExportToCsvAsync(writer, SelectedRun, diffs.Items, includeGrace);
            }

            MessageBox.Show($"Exported {diffs.Items.Count:N0} differences to {dlg.FileName}", "Export Completed", MessageBoxButton.OK, MessageBoxImage.Information);
        }
    }

    private void RunServiceControl(string action)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "powershell.exe",
                Arguments = $"-Command \"Start-Process sc.exe -ArgumentList '{action} S3CompareService' -Verb RunAs\"",
                UseShellExecute = true
            };
            Process.Start(psi);
            StatusMessage = $"Requested service {action}...";
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to request service {action}: {ex.Message}", "Service Control", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void SyncPresetSelection()
    {
        if (SelectedJob == null) return;
        SelectedPresetA = PresetsA.FirstOrDefault(p => !string.IsNullOrEmpty(p.Url) && string.Equals(p.Url, SelectedJob.BucketA.EndpointUrl, StringComparison.OrdinalIgnoreCase));
        SelectedPresetB = PresetsB.FirstOrDefault(p => !string.IsNullOrEmpty(p.Url) && string.Equals(p.Url, SelectedJob.BucketB.EndpointUrl, StringComparison.OrdinalIgnoreCase));
    }
}
