$ErrorActionPreference = "Stop"

# Ensure we are in the project root
$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

Write-Host "Cleaning build files..."

if (Test-Path "bin") { Remove-Item -Recurse -Force "bin" }
if (Test-Path "logs") { Remove-Item -Recurse -Force "logs" }
if (Test-Path "data") { Remove-Item -Recurse -Force "data" }

Write-Host "Clean Complete."
