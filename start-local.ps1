# Brisbane Fuel local collector.
#
#   .\start-local.ps1                       Run the dashboard in this window (Ctrl+C stops it).
#   .\start-local.ps1 -InstallStartupTask   Register a Windows Scheduled Task that runs this
#                                           script hidden at logon for the current user, so
#                                           collection resumes after a restart or sign-out.
#                                           Collection still cannot run while the computer is off.
#   .\start-local.ps1 -RemoveStartupTask    Unregister that task.
#
# Installing or removing the task is the user's decision; neither switch runs by itself.
param([int]$Port = 8000, [switch]$InstallStartupTask, [switch]$RemoveStartupTask)
$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot
$pythonPath = Join-Path $projectRoot '.venv\Scripts\python.exe'
$taskName = 'BrisbaneFuelLocalCollector'

if ($RemoveStartupTask) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "Removed scheduled task '$taskName'."
    return
}
if ($InstallStartupTask) {
    $action = New-ScheduledTaskAction -Execute 'powershell.exe' -WorkingDirectory $projectRoot `
        -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$PSCommandPath`" -Port $Port"
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
    $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings `
        -Description 'Starts the local Brisbane fuel collector at logon.' -Force | Out-Null
    Write-Host "Registered scheduled task '$taskName' (at logon, port $Port)."
    return
}

if (-not (Test-Path -LiteralPath $pythonPath)) {
    throw 'Create the environment first: python -m venv .venv, then .\.venv\Scripts\python.exe -m pip install -r requirements.txt'
}
Set-Location -LiteralPath $projectRoot
Write-Host "Brisbane Fuel: http://127.0.0.1:$Port"
Write-Host 'Leave this process running to keep collecting prices. Press Ctrl+C to stop.'
& $pythonPath -m uvicorn fuel_dashboard:app --host 127.0.0.1 --port $Port
