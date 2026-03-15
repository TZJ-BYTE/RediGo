param(
  [Parameter(Position = 0)]
  [string] $Command,

  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]] $ArgsRest
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function RepoRoot {
  $PSScriptRoot
}

function EnsureDir([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function ReadPid([string]$PidPath) {
  if (-not (Test-Path -LiteralPath $PidPath)) { return $null }
  $raw = (Get-Content -LiteralPath $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1)
  if (-not $raw) { return $null }
  $parsedPid = 0
  if ([int]::TryParse($raw.Trim(), [ref]$parsedPid)) { return $parsedPid }
  return $null
}

function GetProcessPathById([int]$ProcessId) {
  try {
    $p = Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId"
    if ($null -eq $p) { return $null }
    return $p.ExecutablePath
  } catch {
    return $null
  }
}

function IsServerProcess([int]$ProcessId, [string]$ExpectedExe) {
  try {
    $p = Get-Process -Id $ProcessId -ErrorAction Stop
  } catch {
    return $false
  }
  $path = GetProcessPathById $ProcessId
  if (-not $path) { return $true }
  return ([IO.Path]::GetFullPath($path) -ieq [IO.Path]::GetFullPath($ExpectedExe))
}

function Usage {
  $u = @"
用法:
  .\redigo start
  .\redigo stop
  .\redigo restart
  .\redigo status
  .\redigo logs [--follow] [--tail N]
  .\redigo client [host] [port]

说明:
  - start 会自动构建 bin\redigo-server.exe (调用 scripts\build.ps1)
  - stop 基于 logs\redigo.pid 停止进程
"@
  Write-Host $u
}

$root = RepoRoot
$binDir = Join-Path $root "bin"
$logDir = Join-Path $root "logs"
$pidPath = Join-Path $logDir "redigo.pid"
$serverExe = Join-Path $binDir "redigo-server.exe"
$clientExe = Join-Path $binDir "redigo-client.exe"
$stdoutPath = Join-Path $logDir "server.out.log"
$stderrPath = Join-Path $logDir "server.err.log"

EnsureDir $logDir
EnsureDir $binDir

if (-not $Command -or $Command -in @("help", "-h", "--help", "/?")) {
  Usage
  exit 0
}

switch ($Command.ToLowerInvariant()) {
  "start" {
    $existingPid = ReadPid $pidPath
    if ($existingPid -and (IsServerProcess $existingPid $serverExe)) {
      Write-Host "已在运行: pid=$existingPid"
      exit 0
    }

    $buildScript = Join-Path $root "scripts\build.ps1"
    if (Test-Path -LiteralPath $buildScript) {
      & powershell -NoProfile -ExecutionPolicy Bypass -File $buildScript | Out-Host
    }

    if (-not (Test-Path -LiteralPath $serverExe)) {
      throw "找不到服务端可执行文件: $serverExe"
    }

    $p = Start-Process -FilePath $serverExe -WorkingDirectory $root -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
    Set-Content -LiteralPath $pidPath -Value $p.Id -Encoding ascii

    Start-Sleep -Milliseconds 200
    if (IsServerProcess $p.Id $serverExe) {
      Write-Host "启动成功: pid=$($p.Id)"
      exit 0
    }
    throw "启动失败: pid=$($p.Id)"
  }

  "stop" {
    $serverPid = ReadPid $pidPath
    if (-not $serverPid) {
      Write-Host "未找到 pid 文件: $pidPath"
      exit 0
    }

    if (-not (IsServerProcess $serverPid $serverExe)) {
      Remove-Item -LiteralPath $pidPath -ErrorAction SilentlyContinue
      Write-Host "进程不存在或不是当前服务端: pid=$serverPid"
      exit 0
    }

    try {
      Stop-Process -Id $serverPid -ErrorAction Stop
    } catch {
      Stop-Process -Id $serverPid -Force -ErrorAction SilentlyContinue
    }

    Start-Sleep -Milliseconds 300
    if (IsServerProcess $serverPid $serverExe) {
      Stop-Process -Id $serverPid -Force -ErrorAction SilentlyContinue
      Start-Sleep -Milliseconds 200
    }

    Remove-Item -LiteralPath $pidPath -ErrorAction SilentlyContinue
    Write-Host "已停止: pid=$serverPid"
    exit 0
  }

  "restart" {
    & $PSCommandPath stop | Out-Host
    & $PSCommandPath start | Out-Host
    exit 0
  }

  "status" {
    $serverPid = ReadPid $pidPath
    if (-not $serverPid) {
      Write-Host "状态: stopped"
      exit 0
    }
    if (IsServerProcess $serverPid $serverExe) {
      Write-Host "状态: running (pid=$serverPid)"
      exit 0
    }
    Write-Host "状态: stale pid (pid=$serverPid)"
    exit 0
  }

  "logs" {
    $follow = $false
    $tail = 200
    for ($i = 0; $i -lt $ArgsRest.Count; $i++) {
      $a = $ArgsRest[$i]
      if ($a -eq "--follow") { $follow = $true; continue }
      if ($a -eq "--tail" -and $i + 1 -lt $ArgsRest.Count) {
        $n = 0
        if ([int]::TryParse($ArgsRest[$i + 1], [ref]$n)) { $tail = $n }
        $i++
        continue
      }
    }

    $target = $stdoutPath
    if (-not (Test-Path -LiteralPath $target)) {
      Write-Host "未找到日志文件: $target"
      exit 0
    }
    if ($follow) {
      Get-Content -LiteralPath $target -Tail $tail -Wait
    } else {
      Get-Content -LiteralPath $target -Tail $tail
    }
    exit 0
  }

  "client" {
    $buildScript = Join-Path $root "scripts\build.ps1"
    if (-not (Test-Path -LiteralPath $clientExe) -and (Test-Path -LiteralPath $buildScript)) {
      & powershell -NoProfile -ExecutionPolicy Bypass -File $buildScript | Out-Host
    }
    if (-not (Test-Path -LiteralPath $clientExe)) {
      throw "找不到客户端可执行文件: $clientExe"
    }
    $serverHost = "127.0.0.1"
    $serverPort = "16379"
    if ($ArgsRest.Count -ge 1) { $serverHost = $ArgsRest[0] }
    if ($ArgsRest.Count -ge 2) { $serverPort = $ArgsRest[1] }
    & $clientExe $serverHost $serverPort
    exit $LASTEXITCODE
  }

  default {
    Usage
    exit 1
  }
}
