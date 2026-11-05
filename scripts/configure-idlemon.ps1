<#
.SYNOPSIS
    Turns IdleMon reporting on or off and stores its API key, for an existing S3CompareService install (no reinstall needed).
.DESCRIPTION
    Settings go to <DataDir>\appsettings.Local.json, which a service update never overwrites. The API key is read with a hidden
    prompt and stored as IDLEMON_APIKEY in the service's own environment (registry value 'Environment' of the service);
    it is never written to a file and never printed. The service reads both at start-up, so use -Restart (or restart it yourself).
    Requires an elevated PowerShell session.
.PARAMETER Enable
    Sets IdleMon.Enabled to true in the override file (created if missing; other content is kept).
.PARAMETER Disable
    Sets IdleMon.Enabled to false in the override file.
.PARAMETER SetApiKey
    Prompts for the IdleMon per-tool API key and stores it for the service.
.PARAMETER DataDir
    The service data directory. Defaults to "C:\ProgramData\S3CompareService".
.PARAMETER Restart
    Restarts the service afterwards so the changes take effect.
.EXAMPLE
    .\configure-idlemon.ps1 -Enable -SetApiKey -Restart
#>
param(
    [switch]$Enable,
    [switch]$Disable,
    [switch]$SetApiKey,
    [string]$DataDir = "C:\ProgramData\S3CompareService",
    [switch]$Restart
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'idlemon-config-helpers.ps1')

$ServiceName = "S3CompareService"

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    throw "Administrator privileges are required. Please run this script in an elevated PowerShell session."
}
if ($Enable -and $Disable) { throw "Use either -Enable or -Disable, not both." }
if (-not ($Enable -or $Disable -or $SetApiKey -or $Restart)) {
    throw "Nothing to do. Use -Enable, -Disable, -SetApiKey and/or -Restart."
}

$settingsFile = Get-LocalSettingsPath -DataDir $DataDir

if ($Enable -or $Disable) {
    Set-IdleMonEnabled -Path $settingsFile -Enabled ([bool]$Enable) | Out-Null
    Write-Host "IdleMon $(if ($Enable) { 'enabled' } else { 'disabled' }) in $settingsFile" -ForegroundColor Green
}

if ($SetApiKey) {
    Set-ServiceApiKey -ServiceName $ServiceName
}

if ($Restart) {
    if (-not (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue)) { throw "Service '$ServiceName' is not installed." }
    Write-Host "Restarting '$ServiceName'..." -ForegroundColor Yellow
    Restart-Service -Name $ServiceName -Force
    (Get-Service -Name $ServiceName).WaitForStatus('Running', '00:00:30')
    Write-Host "Service restarted. Check: scripts\status-service.ps1" -ForegroundColor Green
} else {
    Write-Host "Restart the service to apply the change: Restart-Service $ServiceName" -ForegroundColor Yellow
}
