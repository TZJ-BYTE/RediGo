$ErrorActionPreference = "Stop"

# Ensure we are in the project root
$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

Write-Host "======================================"
Write-Host "  Gedis - Go Redis Implementation (Windows)"
Write-Host "======================================"
Write-Host ""

# Check Go
if (!(Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Error "Error: Go not found. Please install Go 1.21+"
    exit 1
}

Write-Host "Go Version:"
go version
Write-Host ""

# Create directories
if (!(Test-Path "logs")) { New-Item -ItemType Directory -Force -Path "logs" | Out-Null }
if (!(Test-Path "data")) { New-Item -ItemType Directory -Force -Path "data" | Out-Null }
if (!(Test-Path "bin")) { New-Item -ItemType Directory -Force -Path "bin" | Out-Null }

# Tidy
Write-Host "Downloading dependencies..."
go mod tidy
Write-Host ""

# Build
Write-Host "Building project..."
& "$PSScriptRoot\build.ps1"
if ($LASTEXITCODE -ne 0) { Write-Error "Build failed"; exit 1 }
Write-Host ""

Write-Host "======================================"
Write-Host "  Build Complete!"
Write-Host "======================================"
Write-Host ""
Write-Host "Executable locations:"
Write-Host "  Server: .\bin\gedis-server.exe"
Write-Host "  Client: .\bin\gedis-client.exe"
Write-Host ""
Write-Host "Usage:"
Write-Host "  1. Run Server: .\bin\gedis-server.exe"
Write-Host "  2. Run Client: .\bin\gedis-client.exe"
Write-Host ""
