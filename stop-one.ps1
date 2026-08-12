<#
Stops a single service that was started by start-all.ps1. Uses taskkill /T to
kill the whole process tree (mvn/npm spawn child processes that a plain
Stop-Process on the parent PowerShell PID would leave running).

Usage: .\stop-one.ps1 -Service order-service
#>
param(
    [Parameter(Mandatory)]
    [ValidateSet('order-service', 'payment-service', 'kitchen-service', 'delivery-service', 'notification-service', 'frontend')]
    [string]$Service
)

$root = $PSScriptRoot
$pidFile = "$root\.run-pids.json"
if (-not (Test-Path $pidFile)) {
    Write-Error "No .run-pids.json found — did you start services with start-all.ps1?"
    exit 1
}

$procIds = Get-Content $pidFile | ConvertFrom-Json
$targetPid = $procIds.$Service
if (-not $targetPid) {
    Write-Error "No recorded PID for '$Service'. It may already be stopped."
    exit 1
}

Write-Host "Stopping $Service (PID $targetPid, full process tree)..." -ForegroundColor Yellow
taskkill /PID $targetPid /T /F 2>$null | Out-Null
Write-Host "Stopped $Service." -ForegroundColor Green
