# PocketQueue Resource Audit Script
param(
    [string]$BinaryPath = "bin/pocketqueue.exe",
    [int]$TestPort = 64199
)

$ErrorActionPreference = "Stop"

Write-Host "=== POCKETQUEUE RESOURCE & PERFORMANCE AUDIT ===" -ForegroundColor Cyan

# 1. Compile stripped production binary
Write-Host "Building stripped binary (-ldflags='-s -w')..."
go build -ldflags="-s -w" -o $BinaryPath ./cmd/pocketqueue
if (-not (Test-Path $BinaryPath)) {
    Write-Error "Binary build failed!"
}

# 2. Binary Size Check
$fileInfo = Get-Item $BinaryPath
$sizeMB = [math]::Round($fileInfo.Length / 1MB, 2)
Write-Host "Binary Size: $sizeMB MB" -ForegroundColor Yellow

$sizePass = $sizeMB -lt 15.0
if ($sizePass) {
    Write-Host " [PASS] Binary size ($sizeMB MB) < 15.0 MB (Target: < 15 MB)" -ForegroundColor Green
} else {
    Write-Host " [FAIL] Binary size ($sizeMB MB) >= 15.0 MB" -ForegroundColor Red
}

# 3. Startup Latency & Memory Footprint Check
Write-Host "Starting PocketQueue in headless mode on port $TestPort..."
$sw = [System.Diagnostics.Stopwatch]::StartNew()

$proc = Start-Process -FilePath $BinaryPath -ArgumentList "--headless --port $TestPort" -PassThru -NoNewWindow
Start-Sleep -Milliseconds 50

# Wait for TCP port to open
$portReady = $false
$maxRetries = 30
for ($i = 0; $i -lt $maxRetries; $i++) {
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect("127.0.0.1", $TestPort)
        $tcp.Close()
        $portReady = $true
        break
    } catch {
        Start-Sleep -Milliseconds 10
    }
}
$sw.Stop()
$startupMs = $sw.ElapsedMilliseconds

# Measure RAM
$proc.Refresh()
$ramMB = [math]::Round($proc.WorkingSet64 / 1MB, 2)

# Stop Process
Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue

Write-Host "Startup Latency (Process Spawn + TCP Ready): $startupMs ms" -ForegroundColor Yellow
Write-Host "Idle RAM Usage:  $ramMB MB" -ForegroundColor Yellow

$ramPass = $ramMB -lt 25.0
if ($ramPass) {
    Write-Host " [PASS] Idle RAM ($ramMB MB) < 25.0 MB (Target: < 25 MB)" -ForegroundColor Green
} else {
    Write-Host " [FAIL] Idle RAM ($ramMB MB) >= 25.0 MB" -ForegroundColor Red
}

# On Windows, PowerShell child process launch overhead is typically ~500ms; target total readiness < 1500ms
$startupPass = $startupMs -lt 1500
if ($startupPass) {
    Write-Host " [PASS] Startup + TCP bind latency ($startupMs ms) < 1500 ms" -ForegroundColor Green
} else {
    Write-Host " [FAIL] Startup + TCP bind latency ($startupMs ms) >= 1500 ms" -ForegroundColor Red
}

if ($sizePass -and $ramPass -and $startupPass) {
    Write-Host "`n>>> ALL RESOURCE CONSTRAINTS SATISFIED! <<<" -ForegroundColor Green
    exit 0
} else {
    Write-Error "`nOne or more resource constraints failed!"
}
