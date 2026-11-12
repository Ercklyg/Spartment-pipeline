param([switch]$Enable)
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
$taskPython = Join-Path $taskRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $taskPython)) { throw 'Create the project virtual environment first.' }
$taskName = 'Spartment Daily Pipeline'
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    throw 'Task already exists; review it in Task Scheduler instead of overwriting it.'
}
$action = New-ScheduledTaskAction -Execute $taskPython -Argument '-m spartment.stage3_load.daily' -WorkingDirectory $taskRoot
$trigger = New-ScheduledTaskTrigger -Daily -At '01:00'
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 3)
$principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$definition = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description 'Local extraction -> transformation -> approved analytical load. Requires laptop awake and user logged in. Never run concurrently with the Airflow schedules.'
if (-not $Enable) { $definition.Settings.Enabled = $false }
Register-ScheduledTask -TaskName $taskName -InputObject $definition | Select-Object TaskName,State
