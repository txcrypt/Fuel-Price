param([int]$Port = 8000)
$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot
$pythonPath = Join-Path $projectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonPath)) {
    throw 'Create the environment first: python -m venv .venv, then .\.venv\Scripts\python.exe -m pip install -r requirements.txt'
}
Set-Location -LiteralPath $projectRoot
Write-Host "Brisbane Fuel: http://127.0.0.1:$Port"
Write-Host 'Leave this process running to keep collecting prices. Press Ctrl+C to stop.'
& $pythonPath -m uvicorn fuel_dashboard:app --host 127.0.0.1 --port $Port
