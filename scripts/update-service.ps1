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
if ([string]::IsNullOrWhiteSpace($PublishSource)) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $projectFile = Join-Path (Join-Path $scriptDir "..\S3CompareService.Worker") "S3CompareService.Worker.csproj"
    if (-not (Test-Path -LiteralPath $projectFile)) {
        throw "Could not locate S3CompareService.Worker.csproj."
    }
    $publishTemp = Join-Path $env:TEMP "S3CompareService_Update"
    dotnet publish $projectFile -c Release -r win-x64 --self-contained false -o $publishTemp
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed with exit code $LASTEXITCODE." }
    Copy-Item -Path (Join-Path $publishTemp "*") -Destination $InstallDir -Recurse -Force
    Remove-Item -Path $publishTemp -Recurse -Force -ErrorAction SilentlyContinue
} else {
    Copy-Item -Path (Join-Path $PublishSource "*") -Destination $InstallDir -Recurse -Force
}

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
