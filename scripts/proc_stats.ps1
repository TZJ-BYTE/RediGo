param(
  [int]$Id = 0,
  [string]$Name = ""
)

$ErrorActionPreference = "Stop"

if ($Id -gt 0) {
  $p = Get-Process -Id $Id -ErrorAction Stop
} elseif ($Name -ne "") {
  $p = Get-Process -Name $Name -ErrorAction Stop | Select-Object -First 1
} else {
  throw "Id or Name is required"
}

$ws = [math]::Round($p.WorkingSet64 / 1MB, 2)
$pm = [math]::Round($p.PrivateMemorySize64 / 1MB, 2)

Write-Output ("WS_MB=" + $ws)
Write-Output ("PM_MB=" + $pm)
Write-Output ("Threads=" + $p.Threads.Count)
Write-Output ("Handles=" + $p.HandleCount)

