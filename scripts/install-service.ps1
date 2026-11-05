<#
.SYNOPSIS
    Installs S3CompareService as a native Windows Service with auto-restart recovery.
.PARAMETER InstallDir
    Target directory for binaries. Defaults to "C:\Program Files\S3CompareService".
.PARAMETER DataDir
    Directory for SQLite portable database. Defaults to "C:\ProgramData\S3CompareService".
    Passed to the worker as --S3Compare:DataDirectory.
.PARAMETER Port
    Local REST API management port. Defaults to 5088. Passed to the worker as --S3Compare:Urls.
.PARAMETER PublishSource
    Optional path to pre-published folder. If omitted, builds & publishes the worker automatically.
.PARAMETER EnableIdleMon
    Turns IdleMon reporting on: creates <DataDir>\appsettings.Local.json with IdleMon.Enabled = true (or sets it in the existing
    file, keeping other content). That file lives outside the install folder, so service updates never overwrite it.
.PARAMETER SetIdleMonApiKey
    Prompts (hidden input) for the IdleMon per-tool API key and stores it as IDLEMON_APIKEY in the service's own environment.
    The key is never a parameter, never written to a file and never printed.
#>
param(
    [string]$InstallDir = "C:\Program Files\S3CompareService",
    [string]$DataDir = "C:\ProgramData\S3CompareService",
    [int]$Port = 5088,
    [string]$PublishSource = "",
    [switch]$EnableIdleMon,
    [switch]$SetIdleMonApiKey
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'idlemon-config-helpers.ps1')

$ServiceName = "S3CompareService"
$DisplayName = "S3 Compare Continuous Audit Service"
$Description = "Continuously audits and monitors Amazon S3 and Wasabi buckets on schedules, logging differences and metadata to a portable SQLite database."

# Check Admin privileges
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    throw "Administrator privileges are required to install Windows Services. Please run this script in an elevated PowerShell session."
}

# Runs sc.exe with each argument as its own token and fails on a non-zero exit code.
function Invoke-Sc {
    & sc.exe @args | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "sc.exe $($args -join ' ') failed with exit code $LASTEXITCODE."
    }
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "         S3CompareService - Windows Service Setup         " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Stop & remove existing service (must happen before binaries are overwritten)
Write-Host "[1/6] Checking existing service registration..." -ForegroundColor Yellow
$existingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
# Deleting the service also deletes its registry key, and with it the service-scoped environment (for example IDLEMON_APIKEY). Keep it.
$savedEnvironment = @()
if ($existingService) { $savedEnvironment = Get-ServiceEnvironment $ServiceName }
if ($existingService) {
    if ($existingService.Status -ne 'Stopped') {
        Write-Host "       Stopping existing service '$ServiceName'..." -ForegroundColor Gray
        Stop-Service -Name $ServiceName -Force
        (Get-Service -Name $ServiceName).WaitForStatus('Stopped', '00:01:00')
    }
    Write-Host "       Removing existing registration..." -ForegroundColor Gray
    Invoke-Sc delete $ServiceName
    $deadline = (Get-Date).AddSeconds(60)
    while (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
        if ((Get-Date) -gt $deadline) {
            throw "Timed out waiting for service '$ServiceName' to be deleted (close services.msc or other tools holding it open)."
        }
        Start-Sleep -Milliseconds 500
    }
}

# 2. Prepare target directories
Write-Host "[2/6] Creating installation & data directories..." -ForegroundColor Yellow
if (-not (Test-Path -LiteralPath $InstallDir)) {
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
}
if (-not (Test-Path -LiteralPath $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
}

# 3. Publish or copy binaries
Write-Host "[3/6] Deploying application binaries..." -ForegroundColor Yellow
if ([string]::IsNullOrWhiteSpace($PublishSource)) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $projectFile = Join-Path (Join-Path $scriptDir "..\S3CompareService.Worker") "S3CompareService.Worker.csproj"
    if (-not (Test-Path -LiteralPath $projectFile)) {
        throw "Could not locate S3CompareService.Worker.csproj. Please provide -PublishSource."
    }
    Write-Host "       Publishing S3CompareService.Worker (Release win-x64)..." -ForegroundColor Gray
    $publishTemp = Join-Path $env:TEMP "S3CompareService_Publish"
    dotnet publish $projectFile -c Release -r win-x64 --self-contained false -o $publishTemp
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed with exit code $LASTEXITCODE." }
    Copy-Item -Path (Join-Path $publishTemp "*") -Destination $InstallDir -Recurse -Force
    Remove-Item -Path $publishTemp -Recurse -Force -ErrorAction SilentlyContinue
} else {
    Copy-Item -Path (Join-Path $PublishSource "*") -Destination $InstallDir -Recurse -Force
}

$ExePath = Join-Path $InstallDir "S3CompareService.Worker.exe"
if (-not (Test-Path -LiteralPath $ExePath)) {
    throw "Executable not found at '$ExePath'!"
}

# 4. Create Windows Service
Write-Host "[4/6] Registering Windows Service '$ServiceName'..." -ForegroundColor Yellow
# The worker reads S3Compare:Urls and S3Compare:DataDirectory from command-line configuration.
# A trailing backslash before the closing quote would escape it, so trim it.
$DataDirArg = $DataDir.TrimEnd('\')
$binPathWithArgs = "`"$ExePath`" --S3Compare:Urls=http://localhost:$Port --S3Compare:DataDirectory=`"$DataDirArg`""

New-Service -Name $ServiceName `
            -BinaryPathName $binPathWithArgs `
            -DisplayName $DisplayName `
            -Description $Description `
            -StartupType Automatic | Out-Null

# Service-scoped environment: restore what a previous install had, then apply the IdleMon switches.
if ($savedEnvironment.Count -gt 0) {
    Set-ServiceEnvironment -Entries $savedEnvironment -ServiceName $ServiceName
    Write-Host "       Restored the service environment of the previous installation ($($savedEnvironment.Count) entr$(if ($savedEnvironment.Count -eq 1) { 'y' } else { 'ies' }))." -ForegroundColor Gray
}
if ($EnableIdleMon) {
    $localSettings = Get-LocalSettingsPath -DataDir $DataDir
    Set-IdleMonEnabled -Path $localSettings -Enabled $true | Out-Null
    Write-Host "       IdleMon enabled in $localSettings" -ForegroundColor Gray
}
if ($SetIdleMonApiKey) {
    Set-ServiceApiKey -ServiceName $ServiceName
}

# 5. Configure failure recovery (auto-restart on crash)
Write-Host "[5/6] Configuring crash recovery actions (restart after 60s)..." -ForegroundColor Yellow
# Reset fail count after 1 day (86400s), restart after 60s on 1st, 2nd, and subsequent crashes.
# sc.exe needs "name=" and its value as separate argv entries.
Invoke-Sc failure $ServiceName reset= 86400 actions= restart/60000/restart/60000/restart/60000
Invoke-Sc failureflag $ServiceName 1

# 6. Start the service
Write-Host "[6/6] Starting '$ServiceName'..." -ForegroundColor Yellow
Start-Service -Name $ServiceName
(Get-Service -Name $ServiceName).WaitForStatus('Running', '00:00:30')

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
Write-Host " Local settings     : $(Get-LocalSettingsPath -DataDir $DataDir) (IdleMon etc.; survives updates)" -ForegroundColor White
Write-Host "==========================================================" -ForegroundColor Green
