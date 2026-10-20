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
        set
        {
            if (SetField(ref _currentActiveScan, value))
            {
                OnPropertyChanged(nameof(HasActiveScan));
                RaiseScanCommandsChanged();
            }
        }
    }
    public bool HasActiveScan => _currentActiveScan != null;

    private void RaiseScanCommandsChanged()
    {
        PauseActiveScanCommand?.RaiseCanExecuteChanged();
        ResumeActiveScanCommand?.RaiseCanExecuteChanged();
        CancelActiveScanCommand?.RaiseCanExecuteChanged();
    }

    private void RaiseJobCommandsChanged()
    {
        DeleteJobCommand?.RaiseCanExecuteChanged();
        RunJobNowCommand?.RaiseCanExecuteChanged();
        SaveJobCommand?.RaiseCanExecuteChanged();
    }

    // Jobs created in the UI that the service has not stored yet
    private readonly HashSet<string> _unsavedJobIds = new();
    private bool IsSelectedJobUnsaved => SelectedJob != null && _unsavedJobIds.Contains(SelectedJob.Id);

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
                RaiseJobCommandsChanged();
                UpdateCurrentActiveScan();
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
            // Only a user pick (not a display sync) may change the job's endpoint/region.
            if (SetField(ref _selectedPresetA, value) && !_syncingPresets && value != null && SelectedJob != null
                && !string.IsNullOrWhiteSpace(value.Url))
            {
                SelectedJob.BucketA.EndpointUrl = value.Url;
                SelectedJob.BucketA.Region = value.Region;
            }
        }
    }

    private S3EndpointPreset? _selectedPresetB;
    public S3EndpointPreset? SelectedPresetB
    {
        get => _selectedPresetB;
        set
        {
            if (SetField(ref _selectedPresetB, value) && !_syncingPresets && value != null && SelectedJob != null
                && !string.IsNullOrWhiteSpace(value.Url))
            {
                SelectedJob.BucketB.EndpointUrl = value.Url;
                SelectedJob.BucketB.Region = value.Region;
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
                if (!_suppressRunReload) _ = SafeAsync(() => LoadRunDetailsAsync(value), "load run details");
                UpdateCurrentActiveScan();
            }
        }
    }
    private bool _suppressRunReload;
    public bool HasSelectedRun => SelectedRun != null;

    public ObservableCollection<BucketMetadataSnapshot> SelectedRunSnapshots { get; } = new();

    // Differences Explorer
    public ObservableCollection<RecordedDifference> Differences { get; } = new();

    private RecordedDifference? _selectedDifference;
    public RecordedDifference? SelectedDifference
    {
        get => _selectedDifference;
        set
        {
            // Ignore the transient null the grid reports while items are replaced.
            if (value == null && _isUpdatingDifferences) return;
            SetField(ref _selectedDifference, value);
        }
    }
    private bool _isUpdatingDifferences;

    private DifferenceType? _filterType = null;
    public DifferenceType? FilterType
    {
        get => _filterType;
        set
        {
            if (SetField(ref _filterType, value))
            {
                OnPropertyChanged(nameof(FilterTypeTag));
                _diffPageIndex = 0;
                _ = SafeAsync(LoadDifferencesAsync, "load differences");
            }
        }
    }

    public string FilterTypeTag
    {
        get => _filterType?.ToString() ?? "All";
        set
        {
            if (string.IsNullOrEmpty(value) || value == "All")
                FilterType = null;
            else if (Enum.TryParse<DifferenceType>(value, out var parsed))
                FilterType = parsed;
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
                _ = SafeAsync(LoadDifferencesAsync, "load differences");
            }
        }
    }

    private CancellationTokenSource? _searchDebounceCts;
    private string _filterSearchKey = string.Empty;
    public string FilterSearchKey
    {
        get => _filterSearchKey;
        set
        {
            if (SetField(ref _filterSearchKey, value))
            {
                _diffPageIndex = 0;
                _ = DebouncedLoadDifferencesAsync();
            }
        }
    }

    private async Task DebouncedLoadDifferencesAsync()
    {
        _searchDebounceCts?.Cancel();
        _searchDebounceCts?.Dispose();
        var cts = new CancellationTokenSource();
        _searchDebounceCts = cts;

        try
        {
            await Task.Delay(200, cts.Token);
            await SafeAsync(LoadDifferencesAsync, "load differences");
        }
        catch (OperationCanceledException)
        {
        }
    }

    private int _diffPageIndex = 0;
    private int _diffPageSize = 50;
    private int _diffTotalCount = 0;
    private long _diffRequestVersion = 0;
    private bool _isPolling = false;
    private bool _wasScanActive = false;

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
    public RelayCommand TestDailyDigestCommand { get; }

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
        RestartServiceCommand = new RelayCommand(() => RunServiceControl("restart"));

        PauseActiveScanCommand = new RelayCommand(
            async () => await RunScanActionAsync(_client.PauseRunAsync, "pause"),
            () => CurrentActiveScan != null && !CurrentActiveScan.IsPaused);
        ResumeActiveScanCommand = new RelayCommand(
            async () => await RunScanActionAsync(_client.ResumeRunAsync, "resume"),
            () => CurrentActiveScan != null && CurrentActiveScan.IsPaused);
        CancelActiveScanCommand = new RelayCommand(
            async () => await RunScanActionAsync(_client.CancelRunAsync, "cancel"),
            () => CurrentActiveScan != null);

        NewJobCommand = new RelayCommand(CreateNewJob);
        SaveJobCommand = new RelayCommand(async () => await SaveJobAsync(), () => SelectedJob != null);
        DeleteJobCommand = new RelayCommand(async () => await DeleteJobAsync(), () => SelectedJob != null);
        RunJobNowCommand = new RelayCommand(async () => await TriggerSelectedJobAsync(), () => SelectedJob != null && !IsSelectedJobUnsaved);

        TestConnectionACommand = new RelayCommand(async () => await TestConnectionAsync(true));
        TestConnectionBCommand = new RelayCommand(async () => await TestConnectionAsync(false));
        TestSmtpCommand = new RelayCommand(async () => await TestSmtpAsync());
        TestAzureWebhookCommand = new RelayCommand(async () => await TestAzureWebhookAsync());
        TestDailyDigestCommand = new RelayCommand(async () => await TestDailyDigestAsync());

        RefreshRunsCommand = new RelayCommand(async () => await SafeAsync(LoadRunsAsync, "load runs"));
        RefreshDifferencesCommand = new RelayCommand(async () => await SafeAsync(LoadDifferencesAsync, "load differences"));
        NextDiffPageCommand = new RelayCommand(async () =>
        {
            if ((_diffPageIndex + 1) * _diffPageSize < _diffTotalCount)
            {
                _diffPageIndex++;
                await SafeAsync(LoadDifferencesAsync, "load differences");
            }
        });
        PrevDiffPageCommand = new RelayCommand(async () =>
        {
            if (_diffPageIndex > 0)
            {
                _diffPageIndex--;
                await SafeAsync(LoadDifferencesAsync, "load differences");
            }
        });

        ExportCsvCommand = new RelayCommand(async () => await ExportReportAsync(false));
        ExportJsonCommand = new RelayCommand(async () => await ExportReportAsync(true));
        RefreshLogsCommand = new RelayCommand(async () => await SafeAsync(LoadLogsAsync, "load logs"));

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
        await SafeAsync(LoadJobsAsync, "load jobs");
        await SafeAsync(LoadRunsAsync, "load runs");
        await SafeAsync(LoadLogsAsync, "load logs");
    }

    /// <summary>Runs a load operation, reporting failures in the status bar instead of crashing the UI thread.</summary>
    private async Task SafeAsync(Func<Task> action, string what)
    {
        try { await action(); }
        catch (Exception ex) { StatusMessage = $"Failed to {what}: {ex.Message}"; }
    }

    private async Task RunScanActionAsync(Func<string, Task<(bool Success, string Message)>> call, string verb)
    {
        var scan = CurrentActiveScan;
        if (scan == null) return;
        var (ok, msg) = await call(scan.ScanRunId);
        if (!ok)
        {
            MessageBox.Show($"Could not {verb} the scan:\n{msg}", "Scan Control", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        await RefreshStatusAsync();
    }

    // Last seen counters of the active scan whose differences are displayed; used to skip no-op refreshes.
    private string? _lastPolledScanId;
    private long _lastPolledTotal = -1, _lastPolledFiltered = -1, _lastPolledInFlight = -1;

    private async Task OnTimerTickAsync()
    {
        if (_isPolling) return;
        _isPolling = true;
        try
        {
            await RefreshStatusAsync();
            var active = CurrentActiveScan;
            bool isScanActiveNow = active != null;

            if (active != null)
            {
                // Viewing differences of an active scan: refresh only when its counters actually changed
                if (SelectedRun != null && SelectedRun.Id == active.ScanRunId)
                {
                    bool changed = _lastPolledScanId != active.ScanRunId
                        || _lastPolledTotal != active.TotalDifferences
                        || _lastPolledFiltered != active.FilteredDifferencesCount
                        || _lastPolledInFlight != active.InFlightDifferencesCount;
                    if (changed)
                    {
                        _lastPolledScanId = active.ScanRunId;
                        _lastPolledTotal = active.TotalDifferences;
                        _lastPolledFiltered = active.FilteredDifferencesCount;
                        _lastPolledInFlight = active.InFlightDifferencesCount;
                        await SafeAsync(LoadDifferencesAsync, "refresh differences");
                    }
                }
            }
            else if (_wasScanActive)
            {
                // Active scan just finished: auto-refresh runs (and its differences) and audit logs
                _lastPolledScanId = null;
                await SafeAsync(LoadRunsAsync, "refresh runs");
                await SafeAsync(LoadDifferencesAsync, "refresh differences");
                await SafeAsync(LoadLogsAsync, "refresh logs");
            }

            _wasScanActive = isScanActiveNow;
        }
        finally
        {
            _isPolling = false;
        }
    }

    private ServiceStatusDto? _lastStatusForScan;

    public async Task RefreshStatusAsync()
    {
        var status = await _client.GetStatusAsync();
        ServiceStatus = status;
        _lastStatusForScan = status;
        UpdateCurrentActiveScan();
    }

    /// <summary>Prefers the scan for the selected run, then the selected job, then the first active scan.</summary>
    private void UpdateCurrentActiveScan()
    {
        var scans = _lastStatusForScan?.ActiveScans;
        ActiveScanDto? pick = null;
        if (scans != null && scans.Count > 0)
        {
            if (SelectedRun != null) pick = scans.FirstOrDefault(s => s.ScanRunId == SelectedRun.Id);
            if (pick == null && SelectedJob != null) pick = scans.FirstOrDefault(s => s.JobId == SelectedJob.Id);
            pick ??= scans[0];
        }
        CurrentActiveScan = pick;
        RaiseScanCommandsChanged();
    }

    public async Task LoadJobsAsync()
    {
        string? prevSelectedId = SelectedJob?.Id;
        var jobs = await _client.GetJobsAsync();

        // Keep jobs created in the UI that have not been saved yet
        var unsaved = Jobs.Where(j => _unsavedJobIds.Contains(j.Id) && jobs.All(s => s.Id != j.Id)).ToList();
        _unsavedJobIds.IntersectWith(unsaved.Select(j => j.Id));

        Jobs.Clear();
        foreach (var j in unsaved) Jobs.Add(j);
        foreach (var j in jobs) Jobs.Add(j);

        if (prevSelectedId != null)
        {
            SelectedJob = Jobs.FirstOrDefault(j => j.Id == prevSelectedId) ?? Jobs.FirstOrDefault();
        }
        else if (SelectedJob == null && Jobs.Count > 0)
        {
            SelectedJob = Jobs[0];
        }
        RaiseJobCommandsChanged();
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
        _unsavedJobIds.Add(job.Id);
        Jobs.Insert(0, job);
        SelectedJob = job;
        RaiseJobCommandsChanged();
    }

    public async Task SaveJobAsync()
    {
        var job = SelectedJob;
        if (job == null) return;
        try
        {
            await _client.SaveJobAsync(job);
            _unsavedJobIds.Remove(job.Id);
            StatusMessage = $"Saved job '{job.Name}'.";
            // Reload so the UI shows fresh server-owned values (next run, last status, masked secrets, ...)
            await LoadJobsAsync();
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save job:\n{ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    public async Task DeleteJobAsync()
    {
        var job = SelectedJob;
        if (job == null) return;
        var res = MessageBox.Show($"Are you sure you want to delete job '{job.Name}'?", "Confirm Delete", MessageBoxButton.YesNo, MessageBoxImage.Warning);
        if (res != MessageBoxResult.Yes) return;

        try
        {
            if (_unsavedJobIds.Contains(job.Id))
            {
                // Never stored: just discard locally
                _unsavedJobIds.Remove(job.Id);
            }
            else
            {
                await _client.DeleteJobAsync(job.Id);
            }
            StatusMessage = $"Deleted job '{job.Name}'.";
            Jobs.Remove(job);
            SelectedJob = null;
            await LoadJobsAsync();
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to delete job:\n{ex.Message}", "Delete Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    public async Task TriggerSelectedJobAsync()
    {
        if (SelectedJob == null || IsSelectedJobUnsaved) return;
        var (success, msg) = await _client.TriggerJobAsync(SelectedJob.Id);
        if (success)
        {
            StatusMessage = $"Triggered scan for '{SelectedJob.Name}'.";
            await RefreshStatusAsync();
            await SafeAsync(LoadRunsAsync, "refresh runs");
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

    public async Task TestDailyDigestAsync()
    {
        if (SelectedJob == null) return;
        StatusMessage = "Sending test daily all-clear digest report...";
        var (ok, msg) = await _client.TestDailyDigestAsync(SelectedJob.AlertConfig, SelectedJob);
        if (ok)
        {
            MessageBox.Show($"[SUCCESS] Daily all-clear digest report sent:\n{msg}", "Daily Digest Test", MessageBoxButton.OK, MessageBoxImage.Information);
            StatusMessage = "Daily digest test successful.";
        }
        else
        {
            MessageBox.Show($"[FAILED] Daily all-clear digest report failed:\n{msg}", "Daily Digest Test", MessageBoxButton.OK, MessageBoxImage.Error);
            StatusMessage = "Daily digest test failed.";
        }
    }

    public async Task LoadRunsAsync()
    {
        string? prevSelectedRunId = SelectedRun?.Id;
        var runs = await _client.GetRunsAsync();

        // Update in place (keyed by Id) so the grid keeps its scroll position and selection.
        _suppressRunReload = true;
        try
        {
            for (int i = 0; i < runs.Count; i++)
            {
                var fresh = runs[i];
                if (i < Runs.Count && Runs[i].Id == fresh.Id)
                {
                    Runs[i] = fresh;
                    continue;
                }
                int existing = -1;
                for (int j = i + 1; j < Runs.Count; j++) if (Runs[j].Id == fresh.Id) { existing = j; break; }
                if (existing >= 0)
                {
                    Runs.Move(existing, i);
                    Runs[i] = fresh;
                }
                else
                {
                    Runs.Insert(i, fresh);
                }
            }
            while (Runs.Count > runs.Count) Runs.RemoveAt(Runs.Count - 1);

            // Replacing items clears the grid selection; restore the same run (new instance) without reloading details.
            if (prevSelectedRunId != null)
            {
                var again = Runs.FirstOrDefault(r => r.Id == prevSelectedRunId);
                if (again != null)
                {
                    if (!ReferenceEquals(_selectedRun, again)) { _selectedRun = again; OnPropertyChanged(nameof(SelectedRun)); }
                }
            }
        }
        finally
        {
            _suppressRunReload = false;
        }

        if (prevSelectedRunId == null || Runs.All(r => r.Id != prevSelectedRunId))
        {
            // Selected run vanished (or none selected yet): fall back to the newest run and load it
            SelectedRun = Runs.FirstOrDefault();
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
        await SafeAsync(LoadDifferencesAsync, "load differences");
    }

    public async Task LoadDifferencesAsync()
    {
        if (SelectedRun == null)
        {
            Differences.Clear();
            return;
        }

        long version = ++_diffRequestVersion;

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

        if (version != _diffRequestVersion)
            return;

        _diffTotalCount = page.TotalCount;

        // Merge in place keyed by Id so selection and scroll position survive auto-refresh.
        var selectedId = SelectedDifference?.Id;
        _isUpdatingDifferences = true;
        try
        {
            var items = page.Items;
            for (int i = 0; i < items.Count; i++)
            {
                if (i < Differences.Count && Differences[i].Id == items[i].Id) continue;
                int existing = -1;
                for (int j = i + 1; j < Differences.Count; j++) if (Differences[j].Id == items[i].Id) { existing = j; break; }
                if (existing >= 0) Differences.Move(existing, i);
                else Differences.Insert(i, items[i]);
            }
            while (Differences.Count > items.Count) Differences.RemoveAt(Differences.Count - 1);

        }
        finally
        {
            _isUpdatingDifferences = false;
        }

        // Items with an unchanged Id keep their instance (and so their selection); drop selection if it left the page.
        if (selectedId.HasValue && Differences.All(d => d.Id != selectedId.Value))
            SelectedDifference = null;

        OnPropertyChanged(nameof(PaginationDisplay));
    }

    public async Task LoadLogsAsync()
    {
        var logs = await _client.GetLogsAsync();
        AuditLogs.Clear();
        foreach (var l in logs) AuditLogs.Add(l);
    }

    private static string SanitizeFileName(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return "job";
        var invalid = Path.GetInvalidFileNameChars();
        return new string(input.Select(c => invalid.Contains(c) ? '_' : c).ToArray());
    }

    private async Task ExportReportAsync(bool isJson)
    {
        if (SelectedRun == null)
        {
            MessageBox.Show("Please select a scan run first to export its results.", "Export", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        string safeJobName = SanitizeFileName(SelectedRun.JobName);
        string shortId = SelectedRun.Id.Length >= 8 ? SelectedRun.Id[..8] : SelectedRun.Id;

        var dlg = new SaveFileDialog
        {
            FileName = isJson
                ? $"s3compare_audit_{safeJobName}_{shortId}.json"
                : $"s3compare_audit_{safeJobName}_{shortId}.csv",
            Filter = isJson ? "JSON Files (*.json)|*.json" : "CSV Files (*.csv)|*.csv"
        };

        if (dlg.ShowDialog() == true)
        {
            bool includeGrace = GracePeriodFilterMode != 0;
            try
            {
                StatusMessage = "Exporting report from service...";
                // Use the service's streaming export endpoint (all rows, long timeout) instead of paging.
                await _client.ExportRunAsync(SelectedRun.Id, isJson, includeGrace, dlg.FileName);
                StatusMessage = $"Exported report to {dlg.FileName}";
                MessageBox.Show($"Exported report to {dlg.FileName}", "Export Completed", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                StatusMessage = "Export failed.";
                MessageBox.Show($"Export failed:\n{ex.Message}", "Export", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
    }

    private void RunServiceControl(string action)
    {
        try
        {
            string psCommand = action.ToLowerInvariant() switch
            {
                "start" => "Start-Service -Name 'S3CompareService'",
                "stop" => "Stop-Service -Name 'S3CompareService' -Force",
                "restart" => "Restart-Service -Name 'S3CompareService' -Force",
                _ => $"sc.exe {action} S3CompareService"
            };

            var psi = new ProcessStartInfo
            {
                FileName = "powershell.exe",
                Arguments = $"-NoProfile -WindowStyle Hidden -Command \"{psCommand}\"",
                Verb = "runas",
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
        // Display-only sync: must never write back into the job.
        _syncingPresets = true;
        try
        {
            SelectedPresetA = PresetsA.FirstOrDefault(p => !string.IsNullOrEmpty(p.Url) && string.Equals(p.Url, SelectedJob.BucketA.EndpointUrl, StringComparison.OrdinalIgnoreCase));
            SelectedPresetB = PresetsB.FirstOrDefault(p => !string.IsNullOrEmpty(p.Url) && string.Equals(p.Url, SelectedJob.BucketB.EndpointUrl, StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            _syncingPresets = false;
        }
    }

    private bool _syncingPresets;
}
