<#
.SYNOPSIS
    Uninstalls S3CompareService Windows Service.
.PARAMETER KeepData
    If $true (default), leaves database and logs in C:\ProgramData\S3CompareService.
    Pass -KeepData:$false to remove the data directory as well.
.PARAMETER InstallDir
    Target directory for binaries. Defaults to "C:\Program Files\S3CompareService".
.PARAMETER DataDir
    Directory for SQLite portable database. Defaults to "C:\ProgramData\S3CompareService".
#>
param(
    [switch]$KeepData = $true,
    [string]$InstallDir = "C:\Program Files\S3CompareService",
    [string]$DataDir = "C:\ProgramData\S3CompareService"
)

$ErrorActionPreference = 'Stop'

$ServiceName = "S3CompareService"

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    throw "Administrator privileges are required to uninstall Windows Services. Please run this script in an elevated PowerShell session."
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "         S3CompareService - Service Uninstaller           " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Stop service
$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($service) {
    Write-Host "[1/3] Stopping '$ServiceName'..." -ForegroundColor Yellow
    if ($service.Status -ne 'Stopped') {
        Stop-Service -Name $ServiceName -Force
        (Get-Service -Name $ServiceName).WaitForStatus('Stopped', '00:01:00')
    }

    # 2. Delete service registration
    Write-Host "[2/3] Removing service registration..." -ForegroundColor Yellow
    & sc.exe delete $ServiceName | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "sc.exe delete failed with exit code $LASTEXITCODE." }
    $deadline = (Get-Date).AddSeconds(60)
    while (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
        if ((Get-Date) -gt $deadline) { throw "Timed out waiting for service '$ServiceName' to be deleted." }
        Start-Sleep -Milliseconds 500
    }
} else {
    Write-Host "Service '$ServiceName' is not installed." -ForegroundColor Yellow
}

# 3. Clean files
Write-Host "[3/3] Cleaning installation files..." -ForegroundColor Yellow
if (Test-Path -LiteralPath $InstallDir) {
    try { Remove-Item -LiteralPath $InstallDir -Recurse -Force -ErrorAction Stop }
    catch { Write-Warning "Could not fully remove '$InstallDir': $($_.Exception.Message)" }
}

if (-not $KeepData -and (Test-Path -LiteralPath $DataDir)) {
    Write-Host "       Removing data directory '$DataDir'..." -ForegroundColor Red
    try { Remove-Item -LiteralPath $DataDir -Recurse -Force -ErrorAction Stop }
    catch { Write-Warning "Could not fully remove '$DataDir': $($_.Exception.Message)" }
} else {
    Write-Host "       Preserved database & historical data at '$DataDir'." -ForegroundColor Green
}

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Green
Write-Host " Service Uninstalled Successfully." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
