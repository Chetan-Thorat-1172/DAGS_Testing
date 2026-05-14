<#
.SYNOPSIS
    PI-FLOW Load Test Metrics Monitor
    
.DESCRIPTION
    Polls the /metrics Prometheus endpoint every N seconds, extracts key performance 
    metrics, and logs them to a CSV file for analysis.

.PARAMETER Url
    The PI-FLOW service URL (default: https://eataun-etbvumo-up24263.snowflakecomputing.app)

.PARAMETER IntervalSecs
    Polling interval in seconds (default: 15)

.PARAMETER DurationMins
    Total monitoring duration in minutes (default: 5)

.PARAMETER OutputCsv
    Output CSV path (default: load_test_results_<timestamp>.csv)

.EXAMPLE
    .\load_test_monitor.ps1 -DurationMins 10
    .\load_test_monitor.ps1 -IntervalSecs 10 -DurationMins 5
#>

param(
    [string]$Url = "https://eataun-etbvumo-up24263.snowflakecomputing.app",
    [int]$IntervalSecs = 15,
    [int]$DurationMins = 5,
    [string]$OutputCsv = ""
)

if ($OutputCsv -eq "") {
    $ts = Get-Date -Format "yyyyMMdd-HHmm"
    $OutputCsv = "load_test_results_$ts.csv"
}

$metricsUrl = "$Url/metrics"

# Metric names to extract
$METRICS = @(
    "piflow_scheduler_cycle_duration_seconds_sum",
    "piflow_scheduler_cycle_duration_seconds_count",
    "piflow_worker_pool_size",
    "piflow_worker_active_tasks",
    "piflow_dispatch_channel_depth",
    "piflow_db_pool_open_connections",
    "piflow_db_pool_idle_connections",
    "piflow_tasks_dispatched_total",
    "piflow_tasks_completed_total",
    "piflow_dispatch_overflow_total",
    "piflow_dag_runs_created_total",
    "piflow_dlq_inserted_total",
    "piflow_sla_misses_total",
    "piflow_ingestion_parse_failures_total",
    "piflow_ingestion_files_skipped_total",
    "go_goroutines",
    "go_memstats_alloc_bytes",
    "go_memstats_sys_bytes",
    "process_cpu_seconds_total",
    "process_resident_memory_bytes"
)

function Parse-PrometheusMetrics {
    param([string]$Body)
    
    $result = @{}
    foreach ($line in $Body -split "`n") {
        if ($line.StartsWith("#") -or $line.Trim() -eq "") { continue }
        
        # Handle metrics with labels: metric_name{label="value"} 123.45
        # And simple metrics: metric_name 123.45
        if ($line -match '^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(.+)$') {
            $name = $Matches[1]
            $labels = $Matches[2]
            $value = $Matches[3]
            
            if ($labels) {
                $key = "$name$labels"
            } else {
                $key = $name
            }
            $result[$key] = $value
        }
    }
    return $result
}

function Get-MetricValue {
    param([hashtable]$Metrics, [string]$Name)
    
    # Exact match first
    if ($Metrics.ContainsKey($Name)) {
        return $Metrics[$Name]
    }
    
    # Sum labeled metrics (e.g., piflow_tasks_completed_total with state labels)
    $sum = 0.0
    $found = $false
    foreach ($key in $Metrics.Keys) {
        if ($key.StartsWith($Name + "{") -or $key -eq $Name) {
            $sum += [double]$Metrics[$key]
            $found = $true
        }
    }
    if ($found) { return $sum }
    return "N/A"
}

# CSV header
$header = "timestamp,scheduler_cycle_avg_ms,worker_pool_size,active_tasks,dispatch_queue,db_open_conns,db_idle_conns,tasks_dispatched,tasks_completed,dispatch_overflow,dag_runs_created,dlq_inserted,goroutines,mem_alloc_mb,mem_sys_mb,cpu_seconds,resident_mem_mb,parse_failures,files_skipped"
Set-Content -Path $OutputCsv -Value $header

$totalSamples = [math]::Ceiling(($DurationMins * 60) / $IntervalSecs)
$prevDispatched = $null
$prevCompleted = $null
$prevCpuSecs = $null

Write-Host ""
Write-Host "PI-FLOW Load Test Monitor"
Write-Host "========================="
Write-Host "Endpoint:   $metricsUrl"
Write-Host "Interval:   ${IntervalSecs}s"
Write-Host "Duration:   ${DurationMins} min ($totalSamples samples)"
Write-Host "Output:     $OutputCsv"
Write-Host ""
Write-Host "timestamp              | sched_ms | workers | active | queue | db_open | dispatched | completed | overflow | goroutines | mem_mb"
Write-Host "-----------------------+----------+---------+--------+-------+---------+------------+-----------+----------+------------+-------"

for ($i = 1; $i -le $totalSamples; $i++) {
    $now = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    
    try {
        $response = Invoke-WebRequest -Uri $metricsUrl -UseBasicParsing -TimeoutSec 10
        $metrics = Parse-PrometheusMetrics -Body $response.Content
        
        $cycleSum = Get-MetricValue $metrics "piflow_scheduler_cycle_duration_seconds_sum"
        $cycleCount = Get-MetricValue $metrics "piflow_scheduler_cycle_duration_seconds_count"
        $poolSize = Get-MetricValue $metrics "piflow_worker_pool_size"
        $activeTasks = Get-MetricValue $metrics "piflow_worker_active_tasks"
        $dispatchQueue = Get-MetricValue $metrics "piflow_dispatch_channel_depth"
        $dbOpen = Get-MetricValue $metrics "piflow_db_pool_open_connections"
        $dbIdle = Get-MetricValue $metrics "piflow_db_pool_idle_connections"
        $dispatched = Get-MetricValue $metrics "piflow_tasks_dispatched_total"
        $completed = Get-MetricValue $metrics "piflow_tasks_completed_total"
        $overflow = Get-MetricValue $metrics "piflow_dispatch_overflow_total"
        $runsCreated = Get-MetricValue $metrics "piflow_dag_runs_created_total"
        $dlq = Get-MetricValue $metrics "piflow_dlq_inserted_total"
        $goroutines = Get-MetricValue $metrics "go_goroutines"
        $memAlloc = Get-MetricValue $metrics "go_memstats_alloc_bytes"
        $memSys = Get-MetricValue $metrics "go_memstats_sys_bytes"
        $cpuSecs = Get-MetricValue $metrics "process_cpu_seconds_total"
        $residentMem = Get-MetricValue $metrics "process_resident_memory_bytes"
        $parseFailures = Get-MetricValue $metrics "piflow_ingestion_parse_failures_total"
        $filesSkipped = Get-MetricValue $metrics "piflow_ingestion_files_skipped_total"
        
        # Compute averages
        $cycleAvgMs = "N/A"
        if ($cycleSum -ne "N/A" -and $cycleCount -ne "N/A" -and [double]$cycleCount -gt 0) {
            $cycleAvgMs = [math]::Round(([double]$cycleSum / [double]$cycleCount) * 1000, 1)
        }
        
        $memAllocMb = if ($memAlloc -ne "N/A") { [math]::Round([double]$memAlloc / 1MB, 1) } else { "N/A" }
        $memSysMb = if ($memSys -ne "N/A") { [math]::Round([double]$memSys / 1MB, 1) } else { "N/A" }
        $residentMb = if ($residentMem -ne "N/A") { [math]::Round([double]$residentMem / 1MB, 1) } else { "N/A" }
        
        # Console output (compact)
        $line = "{0,-22} | {1,8} | {2,7} | {3,6} | {4,5} | {5,7} | {6,10} | {7,9} | {8,8} | {9,10} | {10,5}" -f `
            $now, $cycleAvgMs, $poolSize, $activeTasks, $dispatchQueue, $dbOpen, $dispatched, $completed, $overflow, $goroutines, $memAllocMb
        Write-Host $line
        
        # CSV output
        $csv = "$now,$cycleAvgMs,$poolSize,$activeTasks,$dispatchQueue,$dbOpen,$dbIdle,$dispatched,$completed,$overflow,$runsCreated,$dlq,$goroutines,$memAllocMb,$memSysMb,$cpuSecs,$residentMb,$parseFailures,$filesSkipped"
        Add-Content -Path $OutputCsv -Value $csv
        
    } catch {
        Write-Host "$now | ERROR: $($_.Exception.Message)"
        Add-Content -Path $OutputCsv -Value "$now,ERROR,,,,,,,,,,,,,,,,,"
    }
    
    if ($i -lt $totalSamples) {
        Start-Sleep -Seconds $IntervalSecs
    }
}

Write-Host ""
Write-Host "Monitoring complete. Results saved to: $OutputCsv"
Write-Host "Samples collected: $totalSamples"
