<#
Starts all 5 Spring Boot microservices (and, by default, the frontend) each in its
own PowerShell window titled with the service name — so you can watch each one's
logs live, and stop any single one (Ctrl+C in its window, or close the window)
without touching the others.

Prereqs this script does NOT start for you:
  - Kafka:    docker compose up -d
  - Postgres: must already be running locally

Usage:
  .\start-all.ps1                 # 5 backend services + frontend
  .\start-all.ps1 -NoFrontend     # backend services only
#>

param(
    [switch]$NoFrontend,
    [string]$DbUsername = 'postgres',
    [string]$DbPassword = 'admin123'
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

$services = [ordered]@{
    'order-service'        = @{ Path = Join-Path $root 'order-service';        Cmd = 'mvn spring-boot:run'; Port = 8081; IsBackend = $true }
    'payment-service'      = @{ Path = Join-Path $root 'payment-service';      Cmd = 'mvn spring-boot:run'; Port = 8082; IsBackend = $true }
    'kitchen-service'      = @{ Path = Join-Path $root 'kitchen-service';      Cmd = 'mvn spring-boot:run'; Port = 8083; IsBackend = $true }
    'delivery-service'     = @{ Path = Join-Path $root 'delivery-service';     Cmd = 'mvn spring-boot:run'; Port = 8084; IsBackend = $true }
    'notification-service' = @{ Path = Join-Path $root 'notification-service'; Cmd = 'mvn spring-boot:run'; Port = 8085; IsBackend = $true }
}
if (-not $NoFrontend) {
    # Auto npm-install on first run (or whenever node_modules is missing) so `npm run dev`
    # doesn't fail with "'vite' is not recognized" the first time you launch this.
    $frontendCmd = 'if (-not (Test-Path .\node_modules)) { npm install }; npm run dev'
    $services['frontend'] = @{ Path = Join-Path $root 'frontend'; Cmd = $frontendCmd; Port = 5173; IsBackend = $false }
}

function Test-PortOpen {
    param([int]$Port)
    try {
        $result = Test-NetConnection -ComputerName localhost -Port $Port -WarningAction SilentlyContinue
        return $result.TcpTestSucceeded
    }
    catch {
        return $false
    }
}

if (-not (Test-PortOpen -Port 9092)) {
    Write-Warning "Kafka doesn't look reachable on localhost:9092 -- run 'docker compose up -d' first."
}
if (-not (Test-PortOpen -Port 5432)) {
    Write-Warning "Postgres doesn't look reachable on localhost:5432 -- start it first."
}

$procIds = [ordered]@{}

foreach ($name in $services.Keys) {
    $svc = $services[$name]
    Write-Host "Starting $name (port $($svc.Port))..." -ForegroundColor Cyan

    $title = "{0} (:{1})" -f $name, $svc.Port
    if ($svc.IsBackend) {
        # DB_USERNAME/DB_PASSWORD are read by each service's application.yml
        # (defaults to postgres/postgres if not set) -- override here to match
        # your actual local Postgres credentials, e.g.:
        #   .\start-all.ps1 -DbPassword 'yourrealpassword'
        $envPrefix = "`$env:DB_USERNAME='{0}'; `$env:DB_PASSWORD='{1}'; " -f $DbUsername, $DbPassword
    }
    else {
        $envPrefix = ''
    }
    $innerCommand = "`$host.UI.RawUI.WindowTitle = '{0}'; {1}{2}" -f $title, $envPrefix, $svc.Cmd

    $startArgs = @{
        FilePath         = 'powershell.exe'
        ArgumentList     = @('-NoExit', '-Command', $innerCommand)
        WorkingDirectory = $svc.Path
        PassThru         = $true
    }
    $proc = Start-Process @startArgs
    $procIds[$name] = $proc.Id
    Start-Sleep -Milliseconds 400
}

$procIds | ConvertTo-Json | Set-Content -Path (Join-Path $root '.run-pids.json')

Write-Host ""
Write-Host "All services launching in separate windows. PIDs saved to .run-pids.json" -ForegroundColor Green
Write-Host "Stop everything:  .\stop-all.ps1"
Write-Host "Stop just one:    .\stop-one.ps1 -Service order-service"
Write-Host "...or just close / Ctrl+C the window for the one you want to stop."
