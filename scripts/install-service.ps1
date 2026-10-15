<#
.SYNOPSIS
    Installs S3CompareService as a native Windows Service with auto-restart recovery.
.PARAMETER InstallDir
    Target directory for binaries. Defaults to "C:\Program Files\S3CompareService".
.PARAMETER DataDir
    Directory for SQLite portable database. Defaults to "C:\ProgramData\S3CompareService".
.PARAMETER Port
    Local REST API management port. Defaults to 5088.
.PARAMETER PublishSource
    Optional path to pre-published folder. If omitted, builds & publishes the worker automatically.
#>
param(
    [string]$InstallDir = "C:\Program Files\S3CompareService",
    [string]$DataDir = "C:\ProgramData\S3CompareService",
    [int]$Port = 5088,
    [string]$PublishSource = ""
)

$ServiceName = "S3CompareService"
$DisplayName = "S3 Compare Continuous Audit Service"
$Description = "Continuously audits and monitors Amazon S3 and Wasabi buckets on schedules, logging differences and metadata to a portable SQLite database."

# Check Admin privileges
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error "Administrator privileges are required to install Windows Services. Please run this script in an elevated PowerShell session."
    exit 1
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "         S3CompareService - Windows Service Setup         " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Prepare target directories
Write-Host "[1/6] Creating installation & data directories..." -ForegroundColor Yellow
if (-not (Test-Path $InstallDir)) {
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
}
if (-not (Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
}

# 2. Publish or copy binaries
Write-Host "[2/6] Deploying application binaries..." -ForegroundColor Yellow
if ([string]::IsNullOrWhiteSpace($PublishSource)) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $projectDir = Join-Path $scriptDir "..\S3CompareService.Worker"
    if (Test-Path (Join-Path $projectDir "S3CompareService.Worker.csproj")) {
        Write-Host "       Publishing S3CompareService.Worker (Release win-x64)..." -ForegroundColor Gray
        $publishTemp = Join-Path $env:TEMP "S3CompareService_Publish"
        dotnet publish (Join-Path $projectDir "S3CompareService.Worker.csproj") -c Release -r win-x64 --self-contained false -o $publishTemp | Out-Null
        Copy-Item -Path "$publishTemp\*" -Destination $InstallDir -Recurse -Force
        Remove-Item -Path $publishTemp -Recurse -Force -ErrorAction SilentlyContinue
    } else {
        Write-Error "Could not locate S3CompareService.Worker.csproj. Please provide -PublishSource."
        exit 1
    }
} else {
    Copy-Item -Path "$PublishSource\*" -Destination $InstallDir -Recurse -Force
}

$ExePath = Join-Path $InstallDir "S3CompareService.Worker.exe"
if (-not (Test-Path $ExePath)) {
    Write-Error "Executable not found at '$ExePath'!"
    exit 1
}

# 3. Stop & remove existing service if present
Write-Host "[3/6] Checking existing service registration..." -ForegroundColor Yellow
$existingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existingService) {
    Write-Host "       Stopping existing service '$ServiceName'..." -ForegroundColor Gray
    Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
    try { Remove-Service -Name $ServiceName -ErrorAction Stop } catch { & sc.exe delete $ServiceName | Out-Null }
    Start-Sleep -Seconds 1
}

# 4. Create Windows Service
Write-Host "[4/6] Registering Windows Service '$ServiceName'..." -ForegroundColor Yellow
$binPathWithArgs = "`"$ExePath`" --urls http://localhost:$Port"

$createdSuccessfully = $false
try {
    # Use native PowerShell New-Service cmdlet (uses Win32 CreateService API directly)
    New-Service -Name $ServiceName `
                -BinaryPathName $binPathWithArgs `
                -DisplayName $DisplayName `
                -Description $Description `
                -StartupType Automatic `
                -ErrorAction Stop | Out-Null
    $createdSuccessfully = $true
} catch {
    Write-Warning "New-Service cmdlet encountered: $($_.Exception.Message). Falling back to sc.exe..."
    # Fallback to sc.exe with exact quoted parameters
    $scBinArg = "binPath= `"$binPathWithArgs`""
    $scStartArg = "start= auto"
    $scNameArg = "DisplayName= $DisplayName"
    & sc.exe create $ServiceName "$scBinArg" "$scStartArg" "$scNameArg"
    if ($LASTEXITCODE -eq 0) {
        & sc.exe description $ServiceName "$Description" | Out-Null
        $createdSuccessfully = $true
    } else {
        Write-Error "Failed to create service using sc.exe (Exit Code: $LASTEXITCODE)."
        exit 1
    }
}

# 5. Configure failure recovery (auto-restart on crash)
Write-Host "[5/6] Configuring crash recovery actions (restart after 60s)..." -ForegroundColor Yellow
# Reset fail count after 1 day (86400s), restart after 60s on 1st, 2nd, and subsequent crashes
& sc.exe failure $ServiceName "reset= 86400" "actions= restart/60000/restart/60000/restart/60000" | Out-Null
& sc.exe failureflag $ServiceName 1 | Out-Null

# 6. Start the service
Write-Host "[6/6] Starting '$ServiceName'..." -ForegroundColor Yellow
Start-Service -Name $ServiceName
Start-Sleep -Seconds 2

$status = Get-Service -Name $ServiceName
Write-Host ""
Write-Host "==========================================================" -ForegroundColor Green
Write-Host " Installation Succeeded!" -ForegroundColor Green
Write-Host " Service Name       : $ServiceName" -ForegroundColor White
Write-Host " Status             : $($status.Status)" -ForegroundColor White
Write-Host " Executable Path    : $ExePath" -ForegroundColor White
Write-Host " Database Location  : $DataDir\s3compareservice.db" -ForegroundColor White
Write-Host " Management API     : http://localhost:$Port/api/status" -ForegroundColor White
Write-Host " Crash Recovery     : Enabled (Automatic restart on failure)" -ForegroundColor White
Write-Host "==========================================================" -ForegroundColor Green
