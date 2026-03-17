param(
  [string]$OutDir = ".\\bench-results",

  [string]$Target1Name = "redigo",
  [string]$Target1ProcessName = "redigo-server",
  [string]$Target1Host = "127.0.0.1",
  [int]$Target1Port = 16379,

  [string]$Target2Name = "",
  [string]$Target2ProcessName = "redis-server",
  [string]$Target2Host = "127.0.0.1",
  [int]$Target2Port = 6379,

  [string]$BenchExe = ".\\bin\\redigo-bench.exe",
  [int]$SampleSeconds = 1,
  [int]$Keyspace = 200000,
  [int]$DurationSeconds = 20,

  [switch]$StartRedigo,
  [string]$RedigoExe = ".\\bin\\redigo-server.exe",
  [string]$RedigoDataDir = ".\\data",
  [int]$RedigoPort = 16379
)

$ErrorActionPreference = "Stop"

function New-Sparkline {
  param([double[]]$Values, [int]$Width = 40)
  if (-not $Values -or $Values.Count -eq 0) { return "" }
  if ($Width -lt 1) { $Width = 1 }
  $blocks = @(".", ":", "-", "=", "+", "*", "#", "%", "@")
  $min = ($Values | Measure-Object -Minimum).Minimum
  $max = ($Values | Measure-Object -Maximum).Maximum
  if ($max -le $min) { return ($blocks[0] * ( [math]::Min($Width, $Values.Count) )) }

  $step = [math]::Max([math]::Floor($Values.Count / $Width), 1)
  $points = New-Object System.Collections.Generic.List[double]
  for ($i = 0; $i -lt $Values.Count; $i += $step) {
    $slice = $Values[$i..([math]::Min($i + $step - 1, $Values.Count - 1))]
    $avg = ($slice | Measure-Object -Average).Average
    $points.Add([double]$avg) | Out-Null
  }

  $s = ""
  foreach ($v in $points) {
    $norm = ($v - $min) / ($max - $min)
    $idx = [int][math]::Round($norm * ($blocks.Count - 1))
    if ($idx -lt 0) { $idx = 0 }
    if ($idx -ge $blocks.Count) { $idx = $blocks.Count - 1 }
    $s += $blocks[$idx]
  }
  return $s
}

function Parse-BenchOut {
  param([string]$OutPath)
  $text = Get-Content $OutPath -ErrorAction Stop
  $ops = $null
  $p50 = $null
  $p95 = $null
  $p99 = $null
  foreach ($line in $text) {
    if ($line -match '^ops_total=(\d+)\s+ops_per_sec=([0-9.]+)\s+mb_per_sec=([0-9.]+)') {
      $ops = [double]$Matches[2]
    }
    if ($line -match '^latency_per_op_ms\s+p50=([0-9.]+)\s+p95=([0-9.]+)\s+p99=([0-9.]+)') {
      $p50 = [double]$Matches[1]
      $p95 = [double]$Matches[2]
      $p99 = [double]$Matches[3]
    }
  }
  return [pscustomobject]@{
    OpsPerSec = $ops
    P50ms     = $p50
    P95ms     = $p95
    P99ms     = $p99
  }
}

function Summarize-MetricsCsv {
  param([string]$CsvPath)
  $rows = Import-Csv $CsvPath
  if (-not $rows -or $rows.Count -eq 0) {
    return [pscustomobject]@{
      CpuAvg = $null; CpuMax = $null
      WsAvg  = $null; WsMax  = $null
      PmAvg  = $null; PmMax  = $null
      CpuSpark = ""; WsSpark = ""
    }
  }

  $cpu = @($rows | ForEach-Object { [double]$_.cpu_pct })
  $ws  = @($rows | ForEach-Object { [double]$_.ws_mb })

  $cpuAvg = ($cpu | Measure-Object -Average).Average
  $cpuMax = ($cpu | Measure-Object -Maximum).Maximum
  $wsAvg = ($ws | Measure-Object -Average).Average
  $wsMax = ($ws | Measure-Object -Maximum).Maximum
  $pmAvg = (@($rows | ForEach-Object { [double]$_.pm_mb }) | Measure-Object -Average).Average
  $pmMax = (@($rows | ForEach-Object { [double]$_.pm_mb }) | Measure-Object -Maximum).Maximum

  return [pscustomobject]@{
    CpuAvg = [double]$cpuAvg
    CpuMax = [double]$cpuMax
    WsAvg  = [double]$wsAvg
    WsMax  = [double]$wsMax
    PmAvg  = [double]$pmAvg
    PmMax  = [double]$pmMax
    CpuSpark = (New-Sparkline -Values $cpu -Width 40)
    WsSpark  = (New-Sparkline -Values $ws -Width 40)
  }
}

function Run-OneProfile {
  param(
    [string]$TargetName,
    [string]$ProcessName,
    [string]$ServerHost,
    [int]$Port,
    [string]$ProfileName,
    [hashtable]$Profile
  )

  $base = Join-Path $OutDir ($TargetName + "_" + $ProfileName)
  $out = $base + "_out.txt"
  $err = $base + "_err.txt"
  $csv = $base + "_metrics.csv"

  $mode = $Profile.mode
  $clients = [int]$Profile.clients
  $pipeline = [int]$Profile.pipeline
  $valueSize = [int]$Profile.value_size
  $ratioGet = [double]$Profile.ratio_get
  $duration = [string]$Profile.duration

  $args = @(
    ("-host=" + $ServerHost),
    ("-port=" + $Port),
    ("-mode=" + $mode),
    ("-clients=" + $clients),
    ("-pipeline=" + $pipeline),
    ("-duration=" + $duration),
    ("-keyspace=" + $Keyspace),
    ("-value_size=" + $valueSize)
  )

  if ($mode -eq "mixed") {
    $args += ("-ratio_get=" + $ratioGet)
  }

  & .\scripts\bench_and_monitor.ps1 -ProcessName $ProcessName -BenchExe $BenchExe -BenchArgs $args -SampleSeconds $SampleSeconds -OutPath $out -ErrPath $err -CsvPath $csv | Out-Null

  $bench = Parse-BenchOut -OutPath $out
  $metrics = Summarize-MetricsCsv -CsvPath $csv

  return [pscustomobject]@{
    Target     = $TargetName
    Profile    = $ProfileName
    Mode       = $mode
    Clients    = $clients
    Pipeline   = $pipeline
    ValueSize  = $valueSize
    RatioGet   = $ratioGet
    OutPath    = $out
    CsvPath    = $csv
    OpsPerSec  = $bench.OpsPerSec
    P50ms      = $bench.P50ms
    P95ms      = $bench.P95ms
    P99ms      = $bench.P99ms
    CpuAvg     = $metrics.CpuAvg
    CpuMax     = $metrics.CpuMax
    WsAvg      = $metrics.WsAvg
    WsMax      = $metrics.WsMax
    PmAvg      = $metrics.PmAvg
    PmMax      = $metrics.PmMax
    CpuSpark   = $metrics.CpuSpark
    WsSpark    = $metrics.WsSpark
  }
}

function Format-Num {
  param($v, [int]$digits = 2)
  if ($v -eq $null) { return "" }
  return [math]::Round([double]$v, $digits)
}

function Write-Report {
  param([object[]]$Rows, [string]$ReportPath)

  $now = Get-Date
  $md = New-Object System.Collections.Generic.List[string]
  $md.Add("# Benchmark Report") | Out-Null
  $md.Add("") | Out-Null
  $md.Add(("Generated: {0}" -f $now.ToString("yyyy-MM-dd HH:mm:ss"))) | Out-Null
  $md.Add("") | Out-Null
  $md.Add(("Keyspace: {0}, Duration: {1}s, Sample: {2}s" -f $Keyspace, $DurationSeconds, $SampleSeconds)) | Out-Null
  $md.Add("") | Out-Null
  $md.Add("## Summary") | Out-Null
  $md.Add("") | Out-Null
  $md.Add("| Target | Profile | Mode | Clients | Pipeline | Value | GET% | Ops/s | p50(ms) | p95(ms) | p99(ms) | CPU avg/max(%) | WS avg/max(MB) | PM avg/max(MB) |") | Out-Null
  $md.Add("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|") | Out-Null
  foreach ($r in $Rows) {
    $getPct = ""
    if ($r.Mode -eq "mixed") { $getPct = [math]::Round([double]$r.RatioGet * 100, 0) }
    $md.Add(("| {0} | {1} | {2} | {3} | {4} | {5}B | {6} | {7} | {8} | {9} | {10} | {11}/{12} | {13}/{14} | {15}/{16} |" -f
      $r.Target, $r.Profile, $r.Mode, $r.Clients, $r.Pipeline, $r.ValueSize, $getPct,
      [math]::Round([double]$r.OpsPerSec,0),
      (Format-Num $r.P50ms 3), (Format-Num $r.P95ms 3), (Format-Num $r.P99ms 3),
      (Format-Num $r.CpuAvg 2), (Format-Num $r.CpuMax 2),
      (Format-Num $r.WsAvg 2), (Format-Num $r.WsMax 2),
      (Format-Num $r.PmAvg 2), (Format-Num $r.PmMax 2)
    )) | Out-Null
  }
  $md.Add("") | Out-Null

  $md.Add("## Sparklines") | Out-Null
  $md.Add("") | Out-Null
  foreach ($r in $Rows) {
    $md.Add(("### {0} / {1}" -f $r.Target, $r.Profile)) | Out-Null
    $md.Add("") | Out-Null
    $md.Add(("CPU: {0}" -f $r.CpuSpark)) | Out-Null
    $md.Add(("WS : {0}" -f $r.WsSpark)) | Out-Null
    $md.Add("") | Out-Null
    $md.Add(("- out: {0}" -f $r.OutPath.Replace("\","/"))) | Out-Null
    $md.Add(("- metrics: {0}" -f $r.CsvPath.Replace("\","/"))) | Out-Null
    $md.Add("") | Out-Null
  }

  if ($Target2Name -ne "") {
    $md.Add("## Diff (Target2 - Target1)") | Out-Null
    $md.Add("") | Out-Null
    $md.Add("| Profile | Ops/s delta | p95(ms) delta | CPU avg delta | WS avg delta |") | Out-Null
    $md.Add("|---|---:|---:|---:|---:|") | Out-Null
    $profiles = ($Rows | Select-Object -ExpandProperty Profile | Sort-Object -Unique)
    foreach ($p in $profiles) {
      $a = $Rows | Where-Object { $_.Target -eq $Target1Name -and $_.Profile -eq $p } | Select-Object -First 1
      $b = $Rows | Where-Object { $_.Target -eq $Target2Name -and $_.Profile -eq $p } | Select-Object -First 1
      if (-not $a -or -not $b) { continue }
      $md.Add(("| {0} | {1} | {2} | {3} | {4} |" -f
        $p,
        ([math]::Round(([double]$b.OpsPerSec - [double]$a.OpsPerSec),0)),
        (Format-Num ([double]$b.P95ms - [double]$a.P95ms) 3),
        (Format-Num ([double]$b.CpuAvg - [double]$a.CpuAvg) 2),
        (Format-Num ([double]$b.WsAvg - [double]$a.WsAvg) 2)
      )) | Out-Null
    }
    $md.Add("") | Out-Null
  }

  $md | Out-File -FilePath $ReportPath -Encoding utf8
}

function Start-RedigoIfNeeded {
  if (-not $StartRedigo) { return $null }
  [System.Environment]::SetEnvironmentVariable("REDIGO_PERSISTENCE_ENABLED", "false", "Process")
  [System.Environment]::SetEnvironmentVariable("REDIGO_PORT", [string]$RedigoPort, "Process")
  [System.Environment]::SetEnvironmentVariable("REDIGO_DATA_DIR", $RedigoDataDir, "Process")
  $p = Start-Process -FilePath $RedigoExe -PassThru -NoNewWindow
  Start-Sleep -Seconds 2
  return $p
}

function Stop-RedigoIfNeeded {
  param($Proc)
  if (-not $StartRedigo) { return }
  if ($Proc -and -not $Proc.HasExited) {
    try { Stop-Process -Id $Proc.Id -Force } catch {}
  }
}

New-Item -ItemType Directory -Force $OutDir | Out-Null

$profiles = @(
  @{ name = "p1_mixed_256b_c50_p16"; mode = "mixed"; ratio_get = 0.8; clients = 50;  pipeline = 16; duration = ($DurationSeconds.ToString() + "s"); value_size = 256 },
  @{ name = "p2_mixed_256b_c100_p32"; mode = "mixed"; ratio_get = 0.8; clients = 100; pipeline = 32; duration = ($DurationSeconds.ToString() + "s"); value_size = 256 },
  @{ name = "p3_get_256b_c100_p32"; mode = "get";   ratio_get = 1.0; clients = 100; pipeline = 32; duration = ($DurationSeconds.ToString() + "s"); value_size = 256 },
  @{ name = "p4_set_256b_c100_p32"; mode = "set";   ratio_get = 0.0; clients = 100; pipeline = 32; duration = ($DurationSeconds.ToString() + "s"); value_size = 256 },
  @{ name = "p5_mixed_4k_c50_p16";  mode = "mixed"; ratio_get = 0.8; clients = 50;  pipeline = 16; duration = ($DurationSeconds.ToString() + "s"); value_size = 4096 },
  @{ name = "p6_set_4k_c50_p16";    mode = "set";   ratio_get = 0.0; clients = 50;  pipeline = 16; duration = ($DurationSeconds.ToString() + "s"); value_size = 4096 }
)

$redigoProc = Start-RedigoIfNeeded
try {
  $rows = New-Object System.Collections.Generic.List[object]
  foreach ($p in $profiles) {
    $rows.Add((Run-OneProfile -TargetName $Target1Name -ProcessName $Target1ProcessName -ServerHost $Target1Host -Port $Target1Port -ProfileName $p.name -Profile $p)) | Out-Null
  }

  if ($Target2Name -ne "") {
    foreach ($p in $profiles) {
      $rows.Add((Run-OneProfile -TargetName $Target2Name -ProcessName $Target2ProcessName -ServerHost $Target2Host -Port $Target2Port -ProfileName $p.name -Profile $p)) | Out-Null
    }
  }

  $report = Join-Path $OutDir "bench_report.md"
  Write-Report -Rows $rows -ReportPath $report
  Write-Output ("report=" + $report)
} finally {
  Stop-RedigoIfNeeded -Proc $redigoProc
}
