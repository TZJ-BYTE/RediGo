$ErrorActionPreference = "Stop"

# Ensure we are in the project root
$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
Set-Location $ProjectRoot

# Check if Go is available
if (!(Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Error "Error: 'go' command not found. Please ensure Go is installed and added to your PATH."
    Write-Host "Tip: If you just installed Go, you may need to restart your terminal or IDE." -ForegroundColor Yellow
    exit 1
}

Write-Host "Building Server..."
if (!(Test-Path "bin")) { New-Item -ItemType Directory -Path "bin" | Out-Null }
go build -o bin/redigo-server.exe cmd/server/main.go
if ($LASTEXITCODE -ne 0) { Write-Error "Server build failed"; exit 1 }

Write-Host "Building Client..."
go build -o bin/redigo-client.exe cmd/client/main.go
if ($LASTEXITCODE -ne 0) { Write-Error "Client build failed"; exit 1 }

Write-Host "Build Complete."
