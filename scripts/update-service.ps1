<#
.SYNOPSIS
    Updates S3CompareService binaries while safely preserving and backing up the SQLite database.
.PARAMETER InstallDir
    Target directory for binaries. Defaults to "C:\Program Files\S3CompareService".
.PARAMETER DataDir
    Directory for SQLite portable database. Defaults to "C:\ProgramData\S3CompareService".
.PARAMETER PublishSource
    Optional path to pre-published folder. If omitted, builds & publishes the worker automatically.
#>
param(
    [string]$InstallDir = "C:\Program Files\S3CompareService",
    [string]$DataDir = "C:\ProgramData\S3CompareService",
    [string]$PublishSource = ""
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'idlemon-config-helpers.ps1')

$ServiceName = "S3CompareService"

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    throw "Administrator privileges are required to update Windows Services. Please run this script in an elevated PowerShell session."
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "          S3CompareService - Safe Service Update          " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Stop service
Write-Host "[1/5] Stopping '$ServiceName'..." -ForegroundColor Yellow
$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if (-not $service) {
    throw "Service '$ServiceName' is not installed. Run install-service.ps1 first."
}
if ($service.Status -ne 'Stopped') {
    Stop-Service -Name $ServiceName -Force
    (Get-Service -Name $ServiceName).WaitForStatus('Stopped', '00:01:00')
}

# 2. Backup SQLite database (service is stopped, so the DB and its WAL/SHM files are consistent)
Write-Host "[2/5] Creating database snapshot backup..." -ForegroundColor Yellow
$dbPath = Join-Path $DataDir "s3compareservice.db"
if (Test-Path -LiteralPath $dbPath) {
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    foreach ($suffix in @('', '-wal', '-shm')) {
        $src = "$dbPath$suffix"
        if (Test-Path -LiteralPath $src) {
            Copy-Item -LiteralPath $src -Destination "$dbPath.bak_$timestamp$suffix" -Force
        }
    }
    Write-Host "       Database backed up to: $dbPath.bak_$timestamp" -ForegroundColor Green
}

# 3. Publish or copy new binaries
Write-Host "[3/5] Updating binaries..." -ForegroundColor Yellow
$publishTemp = $null
if ([string]::IsNullOrWhiteSpace($PublishSource)) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $projectFile = Join-Path (Join-Path $scriptDir "..\S3CompareService.Worker") "S3CompareService.Worker.csproj"
    if (-not (Test-Path -LiteralPath $projectFile)) {
        throw "Could not locate S3CompareService.Worker.csproj."
    }
    $publishTemp = Join-Path $env:TEMP "S3CompareService_Update"
    dotnet publish $projectFile -c Release -r win-x64 --self-contained false -o $publishTemp
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed with exit code $LASTEXITCODE." }
    $newBinaries = $publishTemp
} else {
    $newBinaries = $PublishSource
}

# Local changes to appsettings.json in the install folder are overwritten by the copy below. Keep a backup, say so, and rescue
# the IdleMon settings (they belong in <DataDir>\appsettings.Local.json, which an update never touches).
$installedSettings = Join-Path $InstallDir "appsettings.json"
$newSettings = Join-Path $newBinaries "appsettings.json"
$localSettings = Get-LocalSettingsPath -DataDir $DataDir
if ((Test-Path -LiteralPath $installedSettings) -and (Test-Path -LiteralPath $newSettings) -and
    ((Get-FileHash -LiteralPath $installedSettings).Hash -ne (Get-FileHash -LiteralPath $newSettings).Hash)) {
    $settingsTimestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $settingsBackup = "$installedSettings.bak_$settingsTimestamp"
    Copy-Item -LiteralPath $installedSettings -Destination $settingsBackup -Force
    Write-Host "       WARNING: the installed appsettings.json differs from the new one and will be replaced." -ForegroundColor Yellow
    Write-Host "                Backup: $settingsBackup" -ForegroundColor Yellow
    Write-Host "                Put local changes in $localSettings (it survives updates; restart the service to apply)." -ForegroundColor Yellow
    try {
        if (Move-IdleMonSectionToLocal -InstalledAppSettings $installedSettings -LocalPath $localSettings) {
            Write-Host "       The installed appsettings.json had IdleMon enabled: its IdleMon section was moved to $localSettings" -ForegroundColor Green
        }
    } catch {
        Write-Host "       Could not move the IdleMon section automatically ($($_.Exception.Message)). Copy it from the backup into $localSettings yourself." -ForegroundColor Red
    }
}

Copy-Item -Path (Join-Path $newBinaries "*") -Destination $InstallDir -Recurse -Force
if ($publishTemp) { Remove-Item -Path $publishTemp -Recurse -Force -ErrorAction SilentlyContinue }

# 4. Start service
Write-Host "[4/5] Starting updated service..." -ForegroundColor Yellow
Start-Service -Name $ServiceName
(Get-Service -Name $ServiceName).WaitForStatus('Running', '00:00:30')

# 5. Check health
Write-Host "[5/5] Verifying service status..." -ForegroundColor Yellow
$status = Get-Service -Name $ServiceName
Write-Host ""
Write-Host "==========================================================" -ForegroundColor Green
Write-Host " Update Completed Successfully!" -ForegroundColor Green
Write-Host " Service Name       : $ServiceName" -ForegroundColor White
Write-Host " Status             : $($status.Status)" -ForegroundColor White
Write-Host "==========================================================" -ForegroundColor Green
