param([string]$LoanAirflowDir = '')
$ErrorActionPreference = 'Stop'
$PaymentsDir = $PSScriptRoot

if (-not (Test-Path (Join-Path $PaymentsDir '.env'))) {
    throw 'Run this installer from the existing payments project with its working .env file.'
}
foreach ($relative in @('scripts/run.py', 'src/payments/airflow_tasks.py', 'airflow/dags/payments_reconciliation_dag.py')) {
    if (-not (Test-Path (Join-Path $PaymentsDir $relative))) { throw "Missing update file: $relative" }
}

if ([string]::IsNullOrWhiteSpace($LoanAirflowDir)) {
    $inspection = & docker inspect loan-risk-platform-airflow-worker-1
    if ($LASTEXITCODE -ne 0) { throw 'Loan Airflow worker could not be inspected.' }
    $details = @($inspection | ConvertFrom-Json)
    $discovered = $details[0].Config.Labels.'com.docker.compose.project.working_dir'
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($discovered)) {
        throw 'Could not discover the loan Airflow folder. Rerun with -LoanAirflowDir and its full path.'
    }
    $LoanAirflowDir = $discovered.Trim()
}
$LoanAirflowDir = (Resolve-Path $LoanAirflowDir).Path
if (-not (Test-Path (Join-Path $LoanAirflowDir 'docker-compose.yaml'))) {
    throw "No docker-compose.yaml found in $LoanAirflowDir"
}

$Override = Join-Path $LoanAirflowDir 'docker-compose.override.yaml'
$Marker = '# Managed by payments Airflow installer'
if (Test-Path $Override) {
    if (-not (Get-Content $Override -Raw).StartsWith($Marker)) {
        throw 'An existing custom docker-compose.override.yaml was found. It was preserved; do not overwrite it.'
    }
}
foreach ($alternative in @('compose.override.yaml','compose.override.yml','docker-compose.override.yml')) {
    if (Test-Path (Join-Path $LoanAirflowDir $alternative)) {
        throw "Another override file exists: $alternative. It was preserved; review the configuration before installing."
    }
}
New-Item -ItemType Directory -Force -Path (Join-Path $PaymentsDir 'data/quarantine') | Out-Null

function Quote-Yaml([string]$Value) { return "'" + $Value.Replace('\','/').Replace("'","''") + "'" }
$ProjectSource = Quote-Yaml $PaymentsDir
$DataSource = Quote-Yaml (Join-Path $PaymentsDir 'data')
$DagSource = Quote-Yaml (Join-Path $PaymentsDir 'airflow/dags/payments_reconciliation_dag.py')
$Yaml = @"
$Marker
# Additive project mounts; the existing loan Airflow configuration is retained.
x-payments-mounts: &payments-mounts
  - type: bind
    source: $ProjectSource
    target: /opt/payments
    read_only: true
  - type: bind
    source: $DataSource
    target: /opt/payments/data
  - type: bind
    source: $DagSource
    target: /opt/airflow/dags/payments_reconciliation_dag.py
    read_only: true
services:
  airflow-worker:
    volumes: *payments-mounts
  airflow-dag-processor:
    volumes: *payments-mounts
  airflow-scheduler:
    volumes: *payments-mounts
  airflow-apiserver:
    volumes: *payments-mounts
  airflow-triggerer:
    volumes: *payments-mounts
  airflow-cli:
    volumes: *payments-mounts
"@
$Previous = if (Test-Path $Override) { [System.IO.File]::ReadAllText($Override) } else { $null }
$Utf8 = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($Override,$Yaml,$Utf8)
Push-Location $LoanAirflowDir
try {
    & docker compose config --quiet
    if ($LASTEXITCODE -ne 0) {
        if ($null -ne $Previous) { [System.IO.File]::WriteAllText($Override,$Previous,$Utf8) }
        else { Remove-Item $Override }
        throw 'Compose validation failed. The previous override was restored.'
    }
} finally { Pop-Location }
Write-Host 'Payments mounts installed. Database passwords were not copied into the override.'
Write-Host "Loan Airflow directory: $LoanAirflowDir"
Write-Host 'When existing DAG runs are idle, run these commands:'
Write-Host ('cd "' + $LoanAirflowDir + '"')
Write-Host 'docker compose up -d'
Write-Host 'Then open http://localhost:8081 and trigger payments_settlement_reconciliation.'
