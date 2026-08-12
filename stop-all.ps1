<#
Stops every service that was started by start-all.ps1 (full process tree per
service, via taskkill /T, so mvn/npm child processes are cleaned up too).
#>
$root = $PSScriptRoot
$pidFile = "$root\.run-pids.json"
if (-not (Test-Path $pidFile)) {
    Write-Error "No .run-pids.json found — did you start services with start-all.ps1?"
    exit 1
}

$procIds = Get-Content $pidFile | ConvertFrom-Json
foreach ($name in $procIds.PSObject.Properties.Name) {
    $targetPid = $procIds.$name
    Write-Host "Stopping $name (PID $targetPid)..." -ForegroundColor Yellow
    taskkill /PID $targetPid /T /F 2>$null | Out-Null
}

Remove-Item $pidFile -ErrorAction SilentlyContinue
Write-Host "All services stopped." -ForegroundColor Green
