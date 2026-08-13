<#
Starts all 5 Spring Boot microservices (and, by default, the frontend) each in its
own PowerShell window titled with the service name — so you can watch each one's
logs live, and stop any single one (Ctrl+C in its window, or close the window)
without touching the others.

Prereqs this script does NOT start for you:
  - Kafka:    docker compose up -d
  - Postgres: must already be running locally

Usage:
  .\start-all.ps1                 # 5 backend services + frontend, debug ports open
  .\start-all.ps1 -NoFrontend     # backend services only
  .\start-all.ps1 -NoDebug        # skip JVM debug agent (slightly faster startup)

Debug ports (suspend=n, loopback-only — attach your IDE at any time):
  order-service        5005
  payment-service      5006
  kitchen-service      5007
  delivery-service     5008
  notification-service 5009
#>

param(
    [switch]$NoFrontend,
    [switch]$NoDebug,
    [string]$DbUsername = 'postgres',
    [string]$DbPassword = 'admin123'
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

# Each backend service gets its own JDWP debug port so you can attach
# an IDE debugger to any one of them independently without port conflicts.
# suspend=n means the JVM starts immediately — attach whenever you want.
$services = [ordered]@{
    'order-service'        = @{ Path = Join-Path $root 'order-service';        Port = 8081; DebugPort = 5005; IsBackend = $true }
    'payment-service'      = @{ Path = Join-Path $root 'payment-service';      Port = 8082; DebugPort = 5006; IsBackend = $true }
    'kitchen-service'      = @{ Path = Join-Path $root 'kitchen-service';      Port = 8083; DebugPort = 5007; IsBackend = $true }
    'delivery-service'     = @{ Path = Join-Path $root 'delivery-service';     Port = 8084; DebugPort = 5008; IsBackend = $true }
    'notification-service' = @{ Path = Join-Path $root 'notification-service'; Port = 8085; DebugPort = 5009; IsBackend = $true }
}
if (-not $NoFrontend) {
    $services['frontend'] = @{ Path = Join-Path $root 'frontend'; Port = 5173; DebugPort = $null; IsBackend = $false }
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

    if ($svc.IsBackend) {
        # MAVEN_OPTS is picked up by the Maven JVM automatically — no quoting issues.
        # spring-boot:run runs in-process (non-forked) so MAVEN_OPTS reaches the app JVM too.
        if (-not $NoDebug) {
            $jvmArgs = '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address={0}' -f $svc.DebugPort
            $envPrefix = "`$env:DB_USERNAME='{0}'; `$env:DB_PASSWORD='{1}'; `$env:MAVEN_OPTS='{2}'; " -f $DbUsername, $DbPassword, $jvmArgs
        } else {
            $envPrefix = "`$env:DB_USERNAME='{0}'; `$env:DB_PASSWORD='{1}'; " -f $DbUsername, $DbPassword
        }

        $cmd = $envPrefix + 'mvn spring-boot:run'
    }
    else {
        # Frontend — no DB env, no debug agent; auto npm-install on first run
        $cmd = 'if (-not (Test-Path .\node_modules)) { npm install }; npm run dev'
    }

    $title        = '{0} (:{1})' -f $name, $svc.Port
    $innerCommand = '`$host.UI.RawUI.WindowTitle = ''{0}''; {1}' -f $title, $cmd
    # Use -f so the title and command are fully expanded before being passed to
    # the child powershell.exe — avoids nested-quote / backtick parse errors.
    $innerCommand = "`$host.UI.RawUI.WindowTitle = '{0}'; {1}" -f $title, $cmd

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
if (-not $NoDebug) {
    Write-Host "Debug agents listening (suspend=n):" -ForegroundColor Yellow
    Write-Host "  order-service        -> localhost:5005"
    Write-Host "  payment-service      -> localhost:5006"
    Write-Host "  kitchen-service      -> localhost:5007"
    Write-Host "  delivery-service     -> localhost:5008"
    Write-Host "  notification-service -> localhost:5009"
    Write-Host ""
}
Write-Host "Stop everything:  .\stop-all.ps1"
Write-Host "Stop just one:    .\stop-one.ps1 -Service order-service"
Write-Host "...or just close / Ctrl+C the window for the one you want to stop."
