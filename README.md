# S3CompareService

**S3CompareService** is a continuous S3 bucket comparison and disparity tracking system designed for enterprise-grade data synchronization auditing between Amazon S3 and S3-compatible cloud storage providers (such as Wasabi).

It runs continuous comparisons on automated schedules (interval or cron), captures bucket metadata snapshots at scan time, shields against transient replication lag false alarms, persists full raw history into a portable SQLite database, and automatically recovers from unexpected crashes or system restarts.

---

## Key Features

- **Continuous Comparison Engine**:
  - Resilient streaming scanner using continuation tokens without caching millions of objects in memory.
  - Supports **Listing Only** (metadata, size, and ETag comparison) and **Full Data** (64KB streaming byte-by-byte comparison).
  - Sequence gap detection for numeric filename series (e.g. `log_001.csv`, `log_003.csv`).
- **Replication Lag Grace Period (In-Flight Replication Shield)**:
  - Configurable window (e.g., 15 minutes) to ignore recent files from causing disparity alerts while replication jobs are actively copying data across regions.
  - Full raw data is **100% preserved** in the database with clipping and tag annotations (`[IN_FLIGHT_REPLICATION]` vs `[CONFIRMED_DISPARITY]`), allowing clean reporting while retaining complete audit trails.
- **Crash Recovery & Ephemeral Data Resilience**:
  - Continuous incremental checkpointing to database every 5 seconds.
  - Service automatically detects interrupted scans upon startup, recovers state, and can resume from `LastEvaluatedKey`.
  - Windows Service configured with automated failure recovery actions (`restart/60000`).
- **Portable SQL Database**:
  - Self-contained SQLite database (`s3compareservice.db`) with Write-Ahead Logging (`WAL` mode) and busy-timeout handling.
  - Stores comparison jobs, execution runs, bucket metadata snapshots, difference records, and audit logs.
  - Zero external database server dependencies. Easily backed up, migrated, or inspected using standard tools.
- **Dual Architecture & Future-Proofing**:
  - Independent Background Worker (`S3CompareService.Worker`) capable of running as a native Windows Service or console process.
  - Embedded REST API (`http://localhost:5088/api`) exposing full health, job, run, and disparity data.
  - Modern Windows UI (`S3CompareService.UI`) built in WPF with a sleek dark theme matching S3Compare.
  - Directly prepared for any future Web UI dashboard consuming the same REST API.
- **Automated PowerShell Lifecycle Scripts**:
  - `install-service.ps1`: Registers Windows Service with auto-restart recovery and creates required folders.
  - `update-service.ps1`: Safely stops service, snapshots/backs up SQLite database, replaces binaries, and restarts.
  - `uninstall-service.ps1`: Safely stops and unregisters the service, optionally preserving data.
  - `status-service.ps1`: Checks service process status and queries API health.

---

## Solution Structure

```
S3CompareService/
├── S3CompareService.Core/        # Domain models, SQLite storage layer, S3 engine, and report exporters
├── S3CompareService.Worker/      # Hostable BackgroundService + REST API + Windows Service integration
├── S3CompareService.UI/          # Windows WPF UI manager (with Dark theme, Live Scan Monitor & Diff Explorer)
├── S3CompareService.Tests/       # Unit & integration test suite (Mock streams, Grace period, Recovery, SQLite)
├── scripts/                      # PowerShell scripts for service installation, update, removal, and status
├── icon.ico & icon.png           # Application icons matching S3Compare
└── README.md
```

---

## Getting Started

### Prerequisites

- Windows 10/11 or Windows Server 2019/2022/2025
- [.NET 9.0 SDK or Runtime](https://dotnet.microsoft.com/download/dotnet/9.0)

### Building the Solution

```powershell
# Clone the repository
git clone https://github.com/lesingerd/S3CompareService.git
cd S3CompareService

# Build all projects
dotnet build

# Run unit tests
dotnet test
```

---

## Service Installation & Management

Run PowerShell as an **Administrator**:

### 1. Install as a Windows Service
```powershell
.\scripts\install-service.ps1
```
*Creates `S3CompareService`, configures automatic crash restart actions (`sc failure reset= 86400 actions= restart/60000`), sets up the portable database at `C:\ProgramData\S3CompareService\s3compareservice.db`, and starts the service.*

### 2. Check Service Status & API Health
```powershell
.\scripts\status-service.ps1
```

### 3. Update Service Binaries (with Safe DB Backup)
```powershell
.\scripts\update-service.ps1
```
*Safely stops the service, creates a timestamped backup of the database (`s3compareservice.db.bak_YYYYMMDD_HHMMSS`), updates binaries, and restarts.*

### 4. Uninstall Service
```powershell
# Uninstalls service while preserving historical database
.\scripts\uninstall-service.ps1

# Or to remove data as well:
.\scripts\uninstall-service.ps1 -KeepData:$false
```

---

## Running Interactively (Console Mode)

For development, testing, or environments without Windows Service permissions:

```powershell
# Run the worker service in console mode
dotnet run --project S3CompareService.Worker

# In another terminal or desktop session, launch the Windows UI
dotnet run --project S3CompareService.UI
```

---

## Windows UI Application

The Windows UI (`S3CompareService.UI`) provides complete control:

1. **Dashboard**: Live status badge, system memory, active scan monitor with real-time speed (`obj/sec`), progress, and pause/resume/cancel controls.
2. **Comparison Jobs**: Add, edit, clone, and configure jobs:
   - S3 credentials for Bucket A and Bucket B (with Wasabi & AWS endpoint presets).
   - "Test Connection" buttons with instant connectivity and object counts feedback.
   - Schedule configuration (Interval minutes or Cron expression).
   - **Replication Lag Shield**: Set grace period in minutes (e.g. 15 minutes) to shield against in-flight replication disparities.
   - Sequence gap detection toggles.
3. **Audit Runs & History**: Timeline of historical scans, durations, objects evaluated, matched counts, confirmed disparities, and bucket metadata snapshots at scan time.
4. **Disparity Explorer**:
   - Filter by difference type (Missing in B, Missing in A, Size mismatch, ETag mismatch, Content mismatch, Sequence gap).
   - Filter by replication status:
     - `Confirmed Disparities Only` (hides in-flight replication lag)
     - `All Raw Differences` (complete unclipped dataset)
     - `In-Flight Lag Only`
   - Side-by-side inspection card comparing Bucket A vs Bucket B object metadata.
5. **Export & Reports**: Export difference sets to CSV or JSON with replication lag filters applied.
6. **Service Audit Logs**: Real-time log viewer for crash recoveries, service startups, and diagnostics.

---

## REST API Reference

The background service hosts a lightweight REST API on `http://localhost:5088`:

| Endpoint | Method | Description |
|---|---|---|
| `/api/status` | `GET` | Service status, uptime, RAM, active scans, and live progress |
| `/api/presets` | `GET` | Available Wasabi and AWS endpoint presets |
| `/api/test-connection` | `POST` | Tests S3 credentials and connectivity for a bucket |
| `/api/list-buckets` | `POST` | Discovers available buckets using provided credentials |
| `/api/jobs` | `GET` / `POST` | List all jobs or create a new comparison job |
| `/api/jobs/{id}` | `GET` / `PUT` / `DELETE` | Retrieve, update, or remove a comparison job |
| `/api/jobs/{id}/run` | `POST` | Trigger an immediate on-demand scan |
| `/api/runs` | `GET` | Retrieve scan execution history (supports `?jobId=` and `?count=`) |
| `/api/runs/{id}` | `GET` | Get run details and summary statistics |
| `/api/runs/{id}/snapshots` | `GET` | Get bucket metadata snapshots at scan time |
| `/api/runs/{id}/differences` | `GET` | Get paged differences (`?type=`, `?withinGracePeriod=`, `?searchKey=`) |
| `/api/runs/{id}/export/csv` | `GET` | Download CSV audit report (`?includeGracePeriod=true/false`) |
| `/api/runs/{id}/export/json` | `GET` | Download structured JSON audit dataset |
| `/api/runs/{id}/pause` | `POST` | Pause an active scan |
| `/api/runs/{id}/resume` | `POST` | Resume a paused scan |
| `/api/runs/{id}/cancel` | `POST` | Cancel an active scan |
| `/api/logs` | `GET` | Retrieve service audit and crash recovery logs |

---

## License

MIT License. Designed and developed as an automated continuous audit service based on the S3Compare architecture.
