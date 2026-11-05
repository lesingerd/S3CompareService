# Shared helpers for the IdleMon settings. Dot-source this file; it does nothing by itself.
#   . (Join-Path $PSScriptRoot 'idlemon-config-helpers.ps1')
#
# IdleMon settings live in <DataDir>\appsettings.Local.json, NOT in the install folder, so a service update
# (which overwrites the install folder) never removes them. The service reads the file at start-up only.

$script:LocalSettingsFileName = 'appsettings.Local.json'
$script:DefaultServiceName    = 'S3CompareService'

function Get-LocalSettingsPath {
    param([Parameter(Mandatory)][string]$DataDir)
    Join-Path $DataDir.Trim().Trim('"') $script:LocalSettingsFileName
}

function ConvertTo-Hashtable {
    # Converts a parsed JSON object (PSCustomObject) into nested ordered hashtables so keys can be added or changed.
    param($InputObject)
    if ($null -eq $InputObject) { return $null }
    if ($InputObject -is [System.Management.Automation.PSCustomObject]) {
        $h = [ordered]@{}
        foreach ($p in $InputObject.PSObject.Properties) { $h[$p.Name] = ConvertTo-Hashtable $p.Value }
        return $h
    }
    if ($InputObject -is [System.Collections.IList] -and $InputObject -isnot [string]) {
        return , @($InputObject | ForEach-Object { ConvertTo-Hashtable $_ })
    }
    return $InputObject
}

function Read-JsonFile {
    # Returns an ordered hashtable, or $null when the file does not exist. Throws on invalid JSON (the file is never overwritten then).
    param([Parameter(Mandatory)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    $text = Get-Content -LiteralPath $Path -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($text)) { return [ordered]@{} }
    try { $obj = $text | ConvertFrom-Json } catch { throw "'$Path' is not valid JSON: $($_.Exception.Message). Fix or remove it and run this again." }
    $h = ConvertTo-Hashtable $obj
    if ($h -isnot [System.Collections.IDictionary]) { throw "'$Path' must contain a JSON object." }
    return $h
}

function Write-JsonFile {
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)]$Value)
    $dir = Split-Path -Parent $Path
    if ($dir -and -not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    $json = $Value | ConvertTo-Json -Depth 20
    [System.IO.File]::WriteAllText($Path, $json + [Environment]::NewLine, (New-Object System.Text.UTF8Encoding($false)))
}

function Set-IdleMonEnabled {
    # Creates <DataDir>\appsettings.Local.json with IdleMon.Enabled, or sets it in the existing file keeping all other content
    # (note: comments in an existing file are not kept). Returns the file path.
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][bool]$Enabled)
    $root = Read-JsonFile -Path $Path
    if ($null -eq $root) { $root = [ordered]@{} }
    if (-not $root.Contains('IdleMon') -or $root['IdleMon'] -isnot [System.Collections.IDictionary]) { $root['IdleMon'] = [ordered]@{} }
    $root['IdleMon']['Enabled'] = $Enabled
    Write-JsonFile -Path $Path -Value $root
    return $Path
}

function Get-IdleMonSection {
    # Reads the IdleMon section of an appsettings.json. Returns $null when the file, or the section, does not exist.
    param([Parameter(Mandatory)][string]$Path)
    $root = Read-JsonFile -Path $Path
    if ($null -eq $root -or -not $root.Contains('IdleMon')) { return $null }
    if ($root['IdleMon'] -isnot [System.Collections.IDictionary]) { return $null }
    return $root['IdleMon']
}

function Move-IdleMonSectionToLocal {
    # Update helper: when the installed appsettings.json had IdleMon enabled and no override file exists yet, copy the
    # IdleMon section into a new appsettings.Local.json. Returns $true when a file was created.
    param([Parameter(Mandatory)][string]$InstalledAppSettings, [Parameter(Mandatory)][string]$LocalPath)
    if (Test-Path -LiteralPath $LocalPath) { return $false }
    $section = Get-IdleMonSection -Path $InstalledAppSettings
    if ($null -eq $section -or $section['Enabled'] -ne $true) { return $false }
    Write-JsonFile -Path $LocalPath -Value ([ordered]@{ IdleMon = $section })
    return $true
}

function Update-EnvironmentEntries {
    # Pure helper for the REG_MULTI_SZ 'Environment' value: returns the entries with NAME=VALUE set, others preserved.
    param([string[]]$Entries, [Parameter(Mandatory)][string]$Name, [Parameter(Mandatory)][string]$Value)
    $kept = @($Entries | Where-Object { $_ -and -not $_.StartsWith("$Name=", [System.StringComparison]::OrdinalIgnoreCase) })
    return , @($kept + "$Name=$Value")
}

function Get-ServiceRegistryPath {
    param([string]$ServiceName = $script:DefaultServiceName)
    "HKLM:\SYSTEM\CurrentControlSet\Services\$ServiceName"
}

function Get-ServiceEnvironment {
    param([string]$ServiceName = $script:DefaultServiceName)
    $key = Get-ServiceRegistryPath $ServiceName
    if (-not (Test-Path -LiteralPath $key)) { return @() }
    $v = (Get-ItemProperty -LiteralPath $key -Name Environment -ErrorAction SilentlyContinue).Environment
    return , @($v | Where-Object { $_ })
}

function Set-ServiceEnvironment {
    param([string[]]$Entries, [string]$ServiceName = $script:DefaultServiceName)
    $key = Get-ServiceRegistryPath $ServiceName
    if (-not (Test-Path -LiteralPath $key)) { throw "Service '$ServiceName' is not installed (registry key not found)." }
    if ($Entries.Count -eq 0) { Remove-ItemProperty -LiteralPath $key -Name Environment -ErrorAction SilentlyContinue }
    else { Set-ItemProperty -LiteralPath $key -Name Environment -Value ([string[]]$Entries) -Type MultiString }
}

function Set-ServiceApiKey {
    # Asks for the key (hidden input) and stores IDLEMON_APIKEY=<key> in the service-scoped environment. Never prints the key.
    param([string]$ServiceName = $script:DefaultServiceName, [string]$EnvName = 'IDLEMON_APIKEY')
    $secure = Read-Host -Prompt "IdleMon per-tool API key for $ServiceName (input is hidden)" -AsSecureString
    $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try {
        $plain = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
        if ([string]::IsNullOrWhiteSpace($plain)) { throw "No key entered; nothing was changed." }
        $entries = Update-EnvironmentEntries -Entries (Get-ServiceEnvironment $ServiceName) -Name $EnvName -Value $plain.Trim()
        Set-ServiceEnvironment -Entries $entries -ServiceName $ServiceName
    }
    finally {
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
        $plain = $null
    }
    Write-Host "       API key stored in the environment of service '$ServiceName' as $EnvName (restart the service to apply)." -ForegroundColor Green
}
