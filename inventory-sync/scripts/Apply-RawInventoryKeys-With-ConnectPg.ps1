#Requires -Version 7.0
param(
    [Parameter(Mandatory=$true)][string]$SourceSnapshot,
    [string]$Database='testdb',
    [int]$LocalPort=15432
)
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$false
$root=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$connect=Join-Path $root 'tools/Connect-Pg.ps1'
$snapshot=(Resolve-Path -LiteralPath $SourceSnapshot).Path
$run=Join-Path $root ('tmp/raw-key-repair/deploy-'+(Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $run | Out-Null
Copy-Item -LiteralPath $snapshot -Destination (Join-Path $run 'source-keys.sql')

$backupQuery=@'
SELECT pg_get_functiondef('operations.normalize_inventory_stage_row()'::regprocedure);
SELECT pg_get_functiondef('operations.refresh_inventory_summary(character varying,character varying,integer)'::regprocedure);
SELECT format('ALTER TABLE %I.%I DROP CONSTRAINT %I; ALTER TABLE %I.%I ADD CONSTRAINT %I %s;',
    n.nspname,c.relname,k.conname,n.nspname,c.relname,k.conname,pg_get_constraintdef(k.oid))
FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='operations' AND k.conname IN
    ('inventory_summary_upc_check','inventory_goods_map_upc_check','inventory_warehouse_map_legacy_wh_id_check');
'@
$before=@(& $connect -Database $Database -LocalPort $LocalPort -NoPasswordPrompt -NoProcessExit `
    -PsqlArgs @('-v','ON_ERROR_STOP=1','-At','-c',$backupQuery) 2>&1 | ForEach-Object { "$_" })
if($LASTEXITCODE -ne 0){throw 'Could not back up current function/constraint definitions'}
$before | Set-Content -LiteralPath (Join-Path $run 'schema-before.sql') -Encoding utf8

$arguments=@('-v','ON_ERROR_STOP=1','-P','pager=off',
    '-f',(Join-Path $root 'inventory-sync/sql/28_pg_preserve_raw_inventory_keys_migration.sql'),
    '-f',$snapshot,
    '-f',(Join-Path $root 'inventory-sync/sql/29_pg_restore_raw_inventory_keys.sql'),
    '-f',(Join-Path $root 'inventory-sync/sql/56_raw_inventory_keys_smoke_test.sql'))
$output=@(& $connect -Database $Database -LocalPort $LocalPort -NoPasswordPrompt -NoProcessExit `
    -PsqlArgs $arguments 2>&1 | ForEach-Object { "$_" })
$status=$LASTEXITCODE
$output | Set-Content -LiteralPath (Join-Path $run 'apply.log') -Encoding utf8
Write-Output ('DATABASE='+$Database)
Write-Output ('DEPLOYMENT_LOG='+$run)
$output | Select-String -Pattern 'restored_component_rows|restored_stage_identities|mem_user_inventory\s+\||mem_user_inventory_lock\s+\||mem_user_inventory_tracking\s+\||NOTICE:.*PASS:|ERROR:|COMMIT|ROLLBACK' |
    ForEach-Object { $_.Line }
if($status -ne 0){throw "Raw-key migration failed (psql exit $status); inspect $run/apply.log"}
Write-Output 'SUCCESS: raw-key schema migration, existing-data restoration and rollback smoke test passed.'
