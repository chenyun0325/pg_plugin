param(
    [string]$Database = 'testdb'
)

$ErrorActionPreference = 'Stop'
$workspaceRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$connectScript = Join-Path $workspaceRoot 'tools/Connect-Pg.ps1'
$masterSql = Join-Path (Split-Path $PSScriptRoot -Parent) 'sql/60_apply_merged_and_test.sql'
$statusPath = Join-Path $workspaceRoot 'tmp/inventory-ddl-apply.status'
$transcriptPath = Join-Path $workspaceRoot 'tmp/inventory-ddl-apply.log'

if (!(Test-Path -LiteralPath $connectScript -PathType Leaf)) {
    throw "Connect-Pg.ps1 not found: $connectScript"
}

Start-Transcript -LiteralPath $transcriptPath -Force | Out-Null
try {
    & $connectScript -Database $Database -NoProcessExit -PsqlArgs @(
        '-v', 'ON_ERROR_STOP=1',
        '-f', $masterSql
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
