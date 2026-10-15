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

$ServiceName = "S3CompareService"

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error "Administrator privileges are required to update Windows Services. Please run this script in an elevated PowerShell session."
    exit 1
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "          S3CompareService - Safe Service Update          " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Stop service
Write-Host "[1/5] Stopping '$ServiceName'..." -ForegroundColor Yellow
$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($service -and $service.Status -ne [System.ServiceProcess.ServiceControllerStatus]::Stopped) {
    Stop-Service -Name $ServiceName -Force
    Start-Sleep -Seconds 2
}

# 2. Backup SQLite database
Write-Host "[2/5] Creating database snapshot backup..." -ForegroundColor Yellow
$dbPath = Join-Path $DataDir "s3compareservice.db"
if (Test-Path $dbPath) {
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $backupPath = Join-Path $DataDir "s3compareservice.db.bak_$timestamp"
    Copy-Item $dbPath $backupPath -Force
    Write-Host "       Database backed up to: $backupPath" -ForegroundColor Green
}

# 3. Publish or copy new binaries
Write-Host "[3/5] Updating binaries..." -ForegroundColor Yellow
if ([string]::IsNullOrWhiteSpace($PublishSource)) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $projectDir = Join-Path $scriptDir "..\S3CompareService.Worker"
    if (Test-Path (Join-Path $projectDir "S3CompareService.Worker.csproj")) {
        $publishTemp = Join-Path $env:TEMP "S3CompareService_Update"
        dotnet publish (Join-Path $projectDir "S3CompareService.Worker.csproj") -c Release -r win-x64 --self-contained false -o $publishTemp | Out-Null
        Copy-Item -Path "$publishTemp\*" -Destination $InstallDir -Recurse -Force
        Remove-Item -Path $publishTemp -Recurse -Force -ErrorAction SilentlyContinue
    } else {
        Write-Error "Could not locate S3CompareService.Worker.csproj."
        exit 1
    }
} else {
    Copy-Item -Path "$PublishSource\*" -Destination $InstallDir -Recurse -Force
}

# 4. Start service
Write-Host "[4/5] Starting updated service..." -ForegroundColor Yellow
Start-Service -Name $ServiceName
Start-Sleep -Seconds 2

# 5. Check health
Write-Host "[5/5] Verifying service status..." -ForegroundColor Yellow
$status = Get-Service -Name $ServiceName
Write-Host ""
Write-Host "==========================================================" -ForegroundColor Green
Write-Host " Update Completed Successfully!" -ForegroundColor Green
Write-Host " Service Name       : $ServiceName" -ForegroundColor White
Write-Host " Status             : $($status.Status)" -ForegroundColor White
Write-Host "==========================================================" -ForegroundColor Green
