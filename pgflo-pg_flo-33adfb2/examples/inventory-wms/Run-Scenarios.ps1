#Requires -Version 7.0
[CmdletBinding()]
param([string]$Go, [switch]$MappingOnly, [switch]$StopAfter)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$moduleRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$repoRoot = Split-Path $moduleRoot -Parent
$reportRoot = Join-Path $repoRoot ('output/pgflo-inventory-wms/' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
$null = New-Item -ItemType Directory -Force -Path $reportRoot
$savedEnv = @{}
foreach ($name in @('PATH','GOCACHE','GOMODCACHE','PGFLO_LAB_BINARY')) {
    $savedEnv[$name] = [Environment]::GetEnvironmentVariable($name)
}
$results = [Collections.Generic.List[object]]::new()
function Invoke-Checked([string]$File, [string[]]$Arguments) {
    & $File @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed ($LASTEXITCODE): $File $($Arguments -join ' ')" }
}
function Run-GoTests([string]$Module, [string]$Label, [string[]]$TestArguments) {
    Push-Location $Module
    try {
        $log = Join-Path $reportRoot ($Label + '.jsonl')
        & $Go test -buildvcs=false -count=1 -json @TestArguments | Tee-Object -FilePath $log | ForEach-Object {
            $event = $_ | ConvertFrom-Json
            if ($event.Test -and $event.Action -in @('pass','fail','skip')) {
                $results.Add([ordered]@{suite=$Label; test=$event.Test; result=$event.Action; seconds=$event.Elapsed})
                Write-Host "$($event.Action.ToUpper()) $($event.Test)"
            }
        }
        if ($LASTEXITCODE -ne 0) { throw "Tests failed: $Label. Details: $log" }
    } finally { Pop-Location }
}

$succeeded = $false
$walExecuted = $false
try {
    if (!$Go) {
        $bundledGo = Join-Path $repoRoot 'tmp/toolchain/go/bin/go.exe'
        $Go = if (Test-Path -LiteralPath $bundledGo) { $bundledGo } else { (Get-Command go -ErrorAction Stop).Source }
    }
    $env:GOCACHE = Join-Path $repoRoot 'tmp/gocache'
    $env:GOMODCACHE = Join-Path $repoRoot 'tmp/gomodcache'
    $env:PGFLO_LAB_BINARY = $null
    Run-GoTests $moduleRoot 'pgflo-unit' @('./...')
    Run-GoTests (Join-Path $repoRoot 'pgcat-yaml') 'pgcat-inventory-wms' @('-run','TestProductionInventorySplitScenarios|TestProductionOutboundCompositeAndDeleteScenarios','.')
    Push-Location $moduleRoot
    try {
        Invoke-Checked $Go @('vet','-buildvcs=false','./...')
        $null = New-Item -ItemType Directory -Force -Path (Join-Path $moduleRoot 'bin')
        $binary = Join-Path $moduleRoot 'bin/pg_flo.exe'
        Invoke-Checked $Go @('build','-buildvcs=false','-o',$binary,'.')
        Invoke-Checked $binary @('job','validate','--job',(Join-Path $PSScriptRoot 'job.yaml'))
        if (!$MappingOnly) {
            $dockerDir = Join-Path $env:LOCALAPPDATA 'Programs/DockerDesktop/resources/bin'
            if (Test-Path -LiteralPath (Join-Path $dockerDir 'docker.exe')) { $env:PATH = "$dockerDir;$env:PATH" }
            $script:docker = (Get-Command docker -ErrorAction Stop).Source
            Invoke-Checked $docker @('compose','version')
            Invoke-Checked $docker @('compose','-f',(Join-Path $PSScriptRoot 'compose.yaml'),'config','--quiet')
            Invoke-Checked $docker @('compose','-f',(Join-Path $PSScriptRoot 'compose.yaml'),'up','-d','--wait','--wait-timeout','120')
            $env:PGFLO_LAB_BINARY = $binary
            $walExecuted = $true
            Run-GoTests $moduleRoot 'pgflo-wal' @('-v','./tests/scenarios','-run','TestInventoryWMSWAL','-timeout','3m')
        }
    } finally { Pop-Location }
    $succeeded = $true
} finally {
    if ($StopAfter -and $script:docker) {
        & $script:docker compose -f (Join-Path $PSScriptRoot 'compose.yaml') stop
    }
    $report = [ordered]@{
        generated_at = (Get-Date).ToString('o')
        succeeded = $succeeded
        wal_executed = $walExecuted
        tests = @($results.ToArray())
        fixture_sha256 = (Get-FileHash (Join-Path $PSScriptRoot 'job.yaml') -Algorithm SHA256).Hash
    }
    $report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $reportRoot 'report.json') -Encoding utf8
    foreach ($name in $savedEnv.Keys) { [Environment]::SetEnvironmentVariable($name,$savedEnv[$name],'Process') }
    Write-Host "Report: $reportRoot/report.json"
}
