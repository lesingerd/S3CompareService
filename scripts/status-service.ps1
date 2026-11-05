param()
$ServiceName = "S3CompareService"
$s = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if (-not $s) {
    Write-Host "Service '$ServiceName' is not installed." -ForegroundColor Yellow
} else {
    Write-Host "Service: $($s.DisplayName)" -ForegroundColor Cyan
    Write-Host "Status : $($s.Status)" -ForegroundColor ($s.Status -eq 'Running' ? 'Green' : 'Red')
    try {
        $api = Invoke-RestMethod -Uri "http://localhost:5088/api/status" -Headers @{ 'X-S3Compare-Client' = 'status-script' } -TimeoutSec 3 -ErrorAction Stop
        Write-Host "API    : Online (Uptime: $($api.Uptime), Active Scans: $($api.ActiveScanCount), RAM: $($api.MemoryMB.ToString('F1')) MB)" -ForegroundColor Green
        try {
            $im = Invoke-RestMethod -Uri "http://localhost:5088/api/idlemon/status" -Headers @{ 'X-S3Compare-Client' = 'status-script' } -TimeoutSec 3 -ErrorAction Stop
            if (-not $im.enabled) {
                $why = if ($im.configProblem) { " ($($im.configProblem))" } else { "" }
                Write-Host "IdleMon: Disabled$why" -ForegroundColor DarkGray
            } else {
                $last = if ($im.lastSendAt) { $im.lastSendAt } else { "never" }
                $err = if ($im.lastError) { "; last error: $($im.lastError)" } else { "" }
                $color = if ($im.lastError) { 'Yellow' } else { 'Green' }
                Write-Host "IdleMon: $($im.mode), last send $last, queue $($im.queueLength), spool $($im.spoolFiles) file(s) / $([math]::Round($im.spoolBytes / 1KB)) KB, accepted $($im.accepted), rejected $($im.rejected)$err" -ForegroundColor $color
            }
        } catch {
            Write-Host "IdleMon: status unavailable" -ForegroundColor DarkGray
        }
    } catch {
        Write-Host "API    : Offline / Unreachable" -ForegroundColor DarkGray
    }
}
