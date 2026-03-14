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

Write-Host "Running tests..."
# Only run tests for packages that have tests
go test -v ./internal/datastruct/... ./internal/persistence/... ./internal/database/...
