param()
$ServiceName = "S3CompareService"
$s = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if (-not $s) {
    Write-Host "Service '$ServiceName' is not installed." -ForegroundColor Yellow
} else {
    Write-Host "Service: $($s.DisplayName)" -ForegroundColor Cyan
    Write-Host "Status : $($s.Status)" -ForegroundColor ($s.Status -eq 'Running' ? 'Green' : 'Red')
    try {
        $api = Invoke-RestMethod -Uri "http://localhost:5088/api/status" -TimeoutSec 3 -ErrorAction Stop
        Write-Host "API    : Online (Uptime: $($api.Uptime), Active Scans: $($api.ActiveScanCount), RAM: $($api.MemoryMB.ToString('F1')) MB)" -ForegroundColor Green
    } catch {
        Write-Host "API    : Offline / Unreachable" -ForegroundColor DarkGray
    }
}
