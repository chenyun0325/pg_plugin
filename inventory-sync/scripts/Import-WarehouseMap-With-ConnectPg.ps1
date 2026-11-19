param(
    [string]$Database = 'testdb'
)

$ErrorActionPreference = 'Stop'
$workspaceRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$connectScript = Join-Path $workspaceRoot 'tools/Connect-Pg.ps1'
$sql = Join-Path (Split-Path $PSScriptRoot -Parent) 'sql/70_import_warehouse_map.sql'
$statusPath = Join-Path $workspaceRoot 'tmp/inventory-warehouse-map-import.status'
$transcriptPath = Join-Path $workspaceRoot 'tmp/inventory-warehouse-map-import.log'

Start-Transcript -LiteralPath $transcriptPath -Force | Out-Null
try {
    & $connectScript -Database $Database -NoProcessExit -PsqlArgs @(
        '-v', 'ON_ERROR_STOP=1',
        '-P', 'pager=off',
        '-f', $sql
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
