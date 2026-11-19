param(
    [string]$Database = 'testdb'
)

$ErrorActionPreference = 'Stop'
$workspaceRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$connectScript = Join-Path $workspaceRoot 'tools/Connect-Pg.ps1'
$smokeSql = Join-Path (Split-Path $PSScriptRoot -Parent) 'sql/50_projection_smoke_test.sql'
$statusPath = Join-Path $workspaceRoot 'tmp/inventory-ddl-verify.status'
$transcriptPath = Join-Path $workspaceRoot 'tmp/inventory-ddl-verify.log'

Start-Transcript -LiteralPath $transcriptPath -Force | Out-Null
try {
    & $connectScript -Database $Database -NoProcessExit -PsqlArgs @(
        '-v', 'ON_ERROR_STOP=1',
        '-f', $smokeSql,
        '-c', "SELECT to_regclass('operations.inventory_summary') AS summary_table, to_regclass('inventory_stage.mem_user_inventory') AS stage_table, to_regclass('inventory_flink_stage.inventory_component') AS component_table"
    )
}
finally {
    Stop-Transcript | Out-Null
}

$pgExitCode = $LASTEXITCODE
if ($pgExitCode -eq 0) {
    Set-Content -LiteralPath $statusPath -Value 'SUCCESS' -Encoding ascii
}
else {
    Set-Content -LiteralPath $statusPath -Value "FAILED:$pgExitCode" -Encoding ascii
}
exit $pgExitCode
