<#
.SYNOPSIS
    Uninstalls S3CompareService Windows Service.
.PARAMETER KeepData
    If $true (default), leaves database and logs in C:\ProgramData\S3CompareService.
    If $false, removes the data directory as well.
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

$ServiceName = "S3CompareService"

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error "Administrator privileges are required to uninstall Windows Services. Please run this script in an elevated PowerShell session."
    exit 1
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "         S3CompareService - Service Uninstaller           " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Stop service
$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($service) {
    Write-Host "[1/3] Stopping '$ServiceName'..." -ForegroundColor Yellow
    if ($service.Status -ne [System.ServiceProcess.ServiceControllerStatus]::Stopped) {
        Stop-Service -Name $ServiceName -Force
        Start-Sleep -Seconds 2
    }

    # 2. Delete service registration
    Write-Host "[2/3] Removing service registration..." -ForegroundColor Yellow
    sc.exe delete $ServiceName | Out-Null
    Start-Sleep -Seconds 1
} else {
    Write-Host "Service '$ServiceName' is not installed." -ForegroundColor Yellow
}

# 3. Clean files
Write-Host "[3/3] Cleaning installation files..." -ForegroundColor Yellow
if (Test-Path $InstallDir) {
    Remove-Item -Path $InstallDir -Recurse -Force -ErrorAction SilentlyContinue
}

if (-not $KeepData -and (Test-Path $DataDir)) {
    Write-Host "       Removing data directory '$DataDir'..." -ForegroundColor Red
    Remove-Item -Path $DataDir -Recurse -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "       Preserved database & historical data at '$DataDir'." -ForegroundColor Green
}

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Green
Write-Host " Service Uninstalled Successfully." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
