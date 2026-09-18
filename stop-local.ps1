# Stops only the background server launched for this workspace.
$ErrorActionPreference = 'Stop'
$pidFile = Join-Path $PSScriptRoot '.local\server.pid'
if (-not (Test-Path -LiteralPath $pidFile)) {
    Write-Host 'No background server recorded. Use Ctrl+C for a foreground server.'
    exit
}
$recordedProcessId = [int](Get-Content -LiteralPath $pidFile)
$processes = Get-CimInstance Win32_Process
$launcher = $processes | Where-Object ProcessId -EQ $recordedProcessId
$expectedPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not $launcher -or $launcher.ExecutablePath -ne $expectedPython -or $launcher.CommandLine -notmatch 'uvicorn fuel_dashboard:app') {
    Write-Host 'The recorded server is no longer running. No processes were stopped.'
    exit
}
$children = $processes | Where-Object { $_.ParentProcessId -eq $recordedProcessId -and $_.CommandLine -match 'uvicorn fuel_dashboard:app' }
foreach ($child in $children) { Stop-Process -Id $child.ProcessId -ErrorAction SilentlyContinue }
Stop-Process -Id $recordedProcessId -ErrorAction SilentlyContinue
Write-Host 'Local fuel collection stopped. Restart with .\start-local.ps1.'
