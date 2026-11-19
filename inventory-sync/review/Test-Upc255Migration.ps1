#Requires -Version 7.0
param([ValidateRange(1024,65535)][int]$Port = 56519)

# This harness never consumes deployment DSNs. It creates and stops a fresh
# loopback-only PostgreSQL cluster inside the workspace.
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
if ($Port -eq 15432) { throw 'The deployment tunnel port is forbidden for this test.' }
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$physicalRoot = $root
$drive = $null
$started = $false
$run = $null

function Invoke-Check {
    param([string]$File,[string[]]$Arguments,[string]$ExpectedError)
    $output = @(& $File @Arguments 2>&1 | ForEach-Object { "$_" })
    $code = $LASTEXITCODE
    $output | Add-Content -LiteralPath (Join-Path $script:run 'commands.log') -Encoding utf8
    if ($ExpectedError) {
        if ($code -eq 0 -or ($output -join "`n") -notmatch $ExpectedError) {
            throw "Expected error /$ExpectedError/ not observed: $($output -join '`n')"
        }
    } elseif ($code -ne 0) { throw "Failed: $File`n$($output -join '`n')" }
    return ($output -join "`n")
}

function Invoke-LocalSql {
    param([string[]]$Arguments,[string]$ExpectedError)
    Invoke-Check $script:psql (@('-X','-w','-h','127.0.0.1','-p',"$Port",'-U','upc_review',
        '-d','postgres','-v','ON_ERROR_STOP=1','-P','pager=off') + $Arguments) $ExpectedError
}

function Invoke-Control {
    param([string[]]$Arguments)
    $process = Start-Process -FilePath $script:pgCtl -WindowStyle Hidden -PassThru `
        -ArgumentList @($Arguments | ForEach-Object { '"' + $_.Replace('"','\"') + '"' }) `
        -RedirectStandardOutput (Join-Path $script:run 'pgctl.stdout.log') `
        -RedirectStandardError (Join-Path $script:run 'pgctl.stderr.log')
    if (!$process.WaitForExit(45000) -or $process.ExitCode -ne 0) {
        throw 'pg_ctl failed; inspect the isolated run logs.'
    }
}

try {
    if ($root -match '[^\x00-\x7F]') {
        $used = @([IO.DriveInfo]::GetDrives() | ForEach-Object { $_.Name.Substring(0,1) })
        $drive = @('R','S','T','U','V','W','X','Y','Z' | Where-Object { $_ -notin $used }) | Select-Object -First 1
        if (!$drive) { throw 'No unused drive alias for the isolated Unicode-path test.' }
        & subst "${drive}:" $physicalRoot
        if ($LASTEXITCODE -ne 0) { throw 'Could not create temporary drive alias.' }
        $root = "${drive}:\"
    }
    $run = Join-Path $root ('tmp/upc-255-review/' + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $run | Out-Null
    $pgBin = Join-Path $root 'tmp/toolchain/postgresql18-server/bin'
    $psql = Join-Path $pgBin 'psql.exe'
    $pgCtl = Join-Path $pgBin 'pg_ctl.exe'
    $data = Join-Path $run 'data'
    $sqlRoot = Join-Path $root 'inventory-sync/sql'
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,$Port)
    try { $listener.Start() } finally { $listener.Stop() }
    $null = Invoke-Check (Join-Path $pgBin 'initdb.exe') @('-D',$data,'-U','upc_review','-A','trust','--no-locale','-E','UTF8')
    Invoke-Control @('-D',$data,'-l',(Join-Path $run 'postgres.log'),'-o',"-h 127.0.0.1 -p $Port -c wal_level=logical",'-w','start')
    $started = $true
    foreach ($file in @('10_pg_merged.sql','20_pg_merged_projection.sql','15_pg_flink_n_to1_adapter.sql',
                        '21_pg_goods_name_fallback_migration.sql','23_pg_relax_available_quantity_migration.sql',
                        '25_pg_allow_negative_quantities_migration.sql')) {
        $null = Invoke-LocalSql @('-f',(Join-Path $sqlRoot $file))
    }
    $null = Invoke-LocalSql @('-c',@'
ALTER TABLE operations.inventory_goods_map ALTER COLUMN upc TYPE varchar(64);
ALTER TABLE operations.inventory_summary ALTER COLUMN upc TYPE varchar(64);
ALTER TABLE operations.inventory_summary_outbox ALTER COLUMN upc TYPE varchar(64);
'@)
    $null = Invoke-LocalSql @('-c',@'
BEGIN;
INSERT INTO operations.inventory_warehouse_map VALUES ('old-limit-wh',9999);
INSERT INTO inventory_stage.mem_user_inventory(id,upc,uid,wh_id,quantity,last_update_time)
VALUES (9999,'JF446J118821:3920RW-SW:RC1066:3026000188:NETFLIX:DISNEY+:APPLE TV+:PRIME VIDEO:1',0,'old-limit-wh',1,1700000000);
SET CONSTRAINTS ALL IMMEDIATE;
ROLLBACK;
'@) 'UPC exceeds operations.inventory_summary limit'
    $null = Invoke-LocalSql @('-f',(Join-Path $sqlRoot '27_pg_upc_255_migration.sql'))
    foreach ($file in @('55_long_upc_smoke_test.sql','54_signed_quantities_smoke_test.sql','52_zero_owner_smoke_test.sql')) {
        $null = Invoke-LocalSql @('-f',(Join-Path $sqlRoot $file))
    }
    $null = Invoke-LocalSql @('-f',(Join-Path $sqlRoot '27_pg_upc_255_migration.sql'))

    $classes = Join-Path $run 'java'
    New-Item -ItemType Directory -Path $classes | Out-Null
    $null = Invoke-Check 'javac' @('-encoding','UTF-8','-d',$classes,
        (Join-Path $root 'inventory-sync/tools/InventoryGoodsImportBuilder.java'),
        (Join-Path $root 'inventory-sync/tools/InventoryGoodsExport.java'))
    $reported = 'JF446J118821:3920RW-SW:RC1066:3026000188:NETFLIX:DISNEY+:APPLE TV+:PRIME VIDEO:1'
    $cases = @($reported,('A' * 255),([string][char]20013 * 255),('B' * 64 + '-ONE'),('B' * 64 + '-TWO'),('C' * 256),'')
    $gzip = Join-Path $run 'goods-fixture.tsv.gz'
    $stream = [IO.Compression.GZipStream]::new([IO.File]::Create($gzip),[IO.Compression.CompressionLevel]::Optimal)
    $writer = [IO.StreamWriter]::new($stream,[Text.UTF8Encoding]::new($false))
    try {
        $nameHex = [Convert]::ToHexString([Text.Encoding]::UTF8.GetBytes('Fixture goods'))
        foreach ($upc in $cases) {
            $hex = [Convert]::ToHexString([Text.Encoding]::UTF8.GetBytes($upc))
            $writer.WriteLine("$hex`tNULL`t0`t$nameHex")
        }
    } finally { $writer.Dispose() }
    $generated = Join-Path $run 'goods-fixture.sql'
    $builder = Invoke-Check 'java' @('-cp',$classes,'InventoryGoodsImportBuilder',$gzip,$generated)
    if ($builder -notmatch 'EXPORTED=5 SKIPPED_INVALID_UPCS=2') { throw "Unexpected importer stats: $builder" }
    $null = Invoke-LocalSql @('-f',$generated)
    $imported = Invoke-LocalSql @('-At','-c','SELECT count(*),max(length(upc)) FROM operations.inventory_goods_map')
    if ($imported.Trim() -ne '5|255') { throw "Unexpected imported fixture result: $imported" }
    $metadata = Invoke-LocalSql @('-At','-c',@'
SELECT count(*) FROM information_schema.columns
WHERE table_schema='operations' AND table_name IN ('inventory_summary','inventory_goods_map','inventory_summary_outbox')
AND column_name='upc' AND character_maximum_length=255;
'@)
    if ($metadata.Trim() -ne '3') { throw 'UPC widths are not uniformly 255.' }
    'PASS: old error reproduced; long UPC, signed quantity, zero owner, idempotence and importer fixtures passed.'
    'Deployment database was not modified.'
    'TEST_RUN=' + (Join-Path $physicalRoot $run.Substring($root.Length))
} finally {
    try {
        if ($started) { Invoke-Control @('-D',$data,'-m','fast','-w','stop') }
    } finally {
        if ($drive) { & subst "${drive}:" /D }
    }
}
