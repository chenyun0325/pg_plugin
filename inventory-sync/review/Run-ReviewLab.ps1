#Requires -Version 7.0
[CmdletBinding()]
param(
    [string]$PgBin,
    [ValidateRange(1024, 65535)][int]$Port = 56489,
    [string]$Go = 'go'
)

# Always creates a fresh loopback-only cluster. Never consumes deployment DSNs.
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$inventoryRoot = Join-Path $repoRoot 'inventory-sync'
$runRoot = Join-Path $repoRoot ("tmp/inventory-review/{0}" -f [guid]::NewGuid().ToString('N'))
$dataPath = Join-Path $runRoot 'data'
$logPath = Join-Path $runRoot 'commands.log'
$pgLog = Join-Path $runRoot 'postgres.log'
$checks = [Collections.Generic.List[object]]::new()
$started = $false
$succeeded = $false
$failure = $null
$mappedDrive = $null
$exeSuffix = if ($IsWindows) { '.exe' } else { '' }
$savedEnv = @{}
$envNames = @('INVENTORY_MIRROR_DSN', 'INVENTORY_MERGED_DSN', 'INVENTORY_ROLLBACK_DSN',
    'GOCACHE', 'GOMODCACHE', 'PGCLIENTENCODING', 'PGCAT_TEST_DSN', 'PGCAT_TEST_ADMIN_DSN')
foreach ($name in $envNames) { $savedEnv[$name] = [Environment]::GetEnvironmentVariable($name) }

function Invoke-Native {
    param([string]$File, [string[]]$Arguments, [string]$ExpectedError)
    $output = @(& $File @Arguments 2>&1 | ForEach-Object { "$_" })
    $exitCode = $LASTEXITCODE
    $output | Add-Content -LiteralPath $logPath -Encoding utf8
    if ($ExpectedError) {
        if ($exitCode -eq 0 -or ($output -join "`n") -notmatch $ExpectedError) {
            throw "Expected failure /$ExpectedError/ was not observed: $($output -join "`n")"
        }
    } elseif ($exitCode -ne 0) {
        throw "Command failed ($exitCode): $File`n$($output -join "`n")"
    }
    return ($output -join "`n")
}

function Invoke-Sql {
    param([string]$Database, [string]$Sql, [string]$ExpectedError)
    Invoke-Native $script:psql @('-X', '-qAt', '-v', 'ON_ERROR_STOP=1',
        '-d', $script:dsns[$Database], '-c', $Sql) $ExpectedError
}

function Invoke-PgCtl {
    param([string[]]$ControlArguments)
    # pg_ctl start leaves a long-lived child. Avoid inheriting PowerShell's
    # captured pipeline handles, and wait only for pg_ctl, not its descendants.
    $stdoutPath = Join-Path $script:runRoot 'pgctl.stdout.log'
    $stderrPath = Join-Path $script:runRoot 'pgctl.stderr.log'
    $parameters = @{
        FilePath = $script:pgCtl
        ArgumentList = @($ControlArguments | ForEach-Object { '"' + $_.Replace('"', '\"') + '"' })
        RedirectStandardOutput = $stdoutPath
        RedirectStandardError = $stderrPath
        PassThru = $true
    }
    if ($IsWindows) { $parameters.WindowStyle = 'Hidden' }
    $process = Start-Process @parameters
    if (!$process.WaitForExit(45000)) { throw 'pg_ctl did not return within 45 seconds' }
    $stdout = Get-Content -Raw -LiteralPath $stdoutPath -ErrorAction SilentlyContinue
    $stderr = Get-Content -Raw -LiteralPath $stderrPath -ErrorAction SilentlyContinue
    "$stdout`n$stderr" | Add-Content -LiteralPath $script:logPath -Encoding utf8
    if ($process.ExitCode -ne 0) { throw "pg_ctl failed: $stdout`n$stderr" }
}

function Assert-Sql {
    param([string]$Name, [string]$Database, [string]$Sql, [string]$Expected = 't')
    $actual = (Invoke-Sql $Database $Sql).Trim()
    if ($actual -cne $Expected) { throw "$Name expected [$Expected], got [$actual]" }
    $script:checks.Add([ordered]@{ name = $Name; result = 'PASS'; value = $actual })
    Write-Host "PASS $Name"
}

function Invoke-Job {
    param([ValidateSet('forward', 'reverse')][string]$Direction,
        [ValidateSet('check', 'init', 'run')][string]$Action,
        [string]$ExpectedError)
    $config = if ($Direction -eq 'forward') { 'mirror-to-summary-n-to1.yaml' } else { 'summary-to-rollback-1ton.yaml' }
    $arguments = @($Action, '--config', (Join-Path $inventoryRoot "pgcat/$config"))
    if ($Action -eq 'run') { $arguments += '--once' }
    $null = Invoke-Native $script:pgcat $arguments $ExpectedError
}

try {
    # initdb on Windows can fail when its installation/data paths contain CJK.
    # Use a temporary unused drive alias; the actual files stay inside this repo.
    if ($IsWindows -and $repoRoot -match '[^\x00-\x7F]') {
        $physicalRoot = $repoRoot
        $used = @([IO.DriveInfo]::GetDrives() | ForEach-Object { $_.Name.Substring(0,1) })
        $drive = @('R','S','T','U','V','W','X','Y','Z' | Where-Object { $_ -notin $used }) | Select-Object -First 1
        if (!$drive) { throw 'An unused drive letter is required for Windows Unicode path compatibility' }
        & subst "${drive}:" $physicalRoot
        if ($LASTEXITCODE -ne 0) { throw 'Could not create temporary repository drive alias' }
        $mappedDrive = $drive
        $repoRoot = "${drive}:\"
        if ($PgBin -and $PgBin.StartsWith($physicalRoot, [StringComparison]::OrdinalIgnoreCase)) {
            $PgBin = Join-Path $repoRoot $PgBin.Substring($physicalRoot.Length).TrimStart('\','/')
        }
        $inventoryRoot = Join-Path $repoRoot 'inventory-sync'
        $runRoot = Join-Path $repoRoot ("tmp/inventory-review/{0}" -f [guid]::NewGuid().ToString('N'))
        $dataPath = Join-Path $runRoot 'data'
        $logPath = Join-Path $runRoot 'commands.log'
        $pgLog = Join-Path $runRoot 'postgres.log'
    }
    if (!$PgBin) {
        $bundled = Join-Path $repoRoot 'tmp/toolchain/postgresql18-server/bin'
        if (Test-Path -LiteralPath (Join-Path $bundled "initdb$exeSuffix")) { $PgBin = $bundled }
        else {
            $initdbCommand = Get-Command "initdb$exeSuffix" -ErrorAction Stop
            $PgBin = Split-Path $initdbCommand.Source -Parent
        }
    }
    $PgBin = (Resolve-Path -LiteralPath $PgBin).Path
    foreach ($tool in @('initdb', 'pg_ctl', 'psql', 'postgres')) {
        if (!(Test-Path -LiteralPath (Join-Path $PgBin "$tool$exeSuffix"))) {
            throw "PostgreSQL server distribution required; missing $tool in $PgBin"
        }
    }
    $script:psql = Join-Path $PgBin "psql$exeSuffix"
    $pgCtl = Join-Path $PgBin "pg_ctl$exeSuffix"
    # Fail before initdb if this explicitly selected local port is occupied.
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $Port)
    try { $listener.Start() } finally { $listener.Stop() }
    $null = New-Item -ItemType Directory -Path $runRoot
    Write-Host "Isolated review lab: $runRoot"
    $env:PGCLIENTENCODING = 'UTF8'
    $pgVersion = Invoke-Native (Join-Path $PgBin "postgres$exeSuffix") @('--version')
    $goVersion = Invoke-Native $Go @('version')
    $script:pgcat = Join-Path $runRoot "pgcat-yaml$exeSuffix"
    if (!$env:GOCACHE) { $env:GOCACHE = Join-Path $repoRoot 'tmp/gocache' }
    $moduleCache = Join-Path $repoRoot 'tmp/gomodcache'
    if (!$env:GOMODCACHE -and (Test-Path -LiteralPath $moduleCache)) { $env:GOMODCACHE = $moduleCache }
    Push-Location (Join-Path $repoRoot 'pgcat-yaml')
    try { $null = Invoke-Native $Go @('build', '-buildvcs=false', '-o', $script:pgcat, '.') }
    finally { Pop-Location }

    $null = Invoke-Native (Join-Path $PgBin "initdb$exeSuffix") @('-D', $dataPath,
        '-U', 'inv_review', '--auth-local=trust', '--auth-host=trust', '--encoding=UTF8', '--locale=C')
    $options = "-h 127.0.0.1 -p $Port -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8"
    $started = $true
    Invoke-PgCtl @('-D', $dataPath, '-l', $pgLog, '-o', $options, '-w', '-t', '30', 'start')
    $baseDsn = "host=127.0.0.1 port=$Port user=inv_review sslmode=disable connect_timeout=5"
    $script:dsns = @{ admin = "$baseDsn dbname=postgres" }
    foreach ($db in @('mirror', 'merged', 'rollback')) {
        $null = Invoke-Sql admin "CREATE DATABASE inv_review_$db"
        $script:dsns[$db] = "$baseDsn dbname=inv_review_$db"
    }
    $env:INVENTORY_MIRROR_DSN = $script:dsns.mirror
    $env:INVENTORY_MERGED_DSN = $script:dsns.merged
    $env:INVENTORY_ROLLBACK_DSN = $script:dsns.rollback
    foreach ($role in @('mirror', 'merged', 'rollback')) {
        & (Join-Path $inventoryRoot 'scripts/Install-InventorySchema.ps1') -Role $role -Dsn $script:dsns[$role] -Psql $script:psql | Out-Null
    }
    $null = Invoke-Native $script:psql @('-X', '-v', 'ON_ERROR_STOP=1', '-d', $script:dsns.merged,
        '-f', (Join-Path $inventoryRoot 'sql/50_projection_smoke_test.sql'))
    Assert-Sql 'projection smoke rolls back all rows' merged 'SELECT count(*)=0 FROM operations.inventory_summary'
    $null = Invoke-Sql merged @'
INSERT INTO operations.inventory_warehouse_map VALUES ('lab-wh', 1);
INSERT INTO operations.inventory_goods_map VALUES ('lab-upc', 'Review lab goods', 1);
'@
    $null = Invoke-Sql mirror @'
BEGIN;
INSERT INTO inventory_mirror.mem_user_inventory VALUES (101, 'lab-upc', 11, 'lab-wh', 10, 1700000000, NULL, NULL, NULL);
INSERT INTO inventory_mirror.mem_user_inventory_lock VALUES (102, 'lab-upc', 11, 'lab-wh', 3, 1700000001, NULL, NULL, NULL);
INSERT INTO inventory_mirror.mem_user_inventory_tracking VALUES (103, 'lab-upc', 11, 'lab-wh', 2, 1700000002, NULL, NULL, NULL);
COMMIT;
'@
    Invoke-Job forward check
    Invoke-Job forward init
    Assert-Sql 'forward snapshot merges three quantities and timestamp' merged @'
SELECT count(*)=1 AND bool_and((id, on_hand_qty, reserved_qty, hold_qty,
 extract(epoch FROM updated_at)::int)=(101,10,3,2,1700000002)) FROM operations.inventory_summary
'@
    Assert-Sql 'summary and outbox agree' merged @'
SELECT count(*)=1 AND bool_and((on_hand_qty,reserved_qty,hold_qty)=(10,3,2)) FROM operations.inventory_summary_outbox
'@
    $diagnostic = Invoke-Native $script:psql @('-X', '-qAt', '-v', 'ON_ERROR_STOP=1',
        '-d', $script:dsns.merged, '-f', (Join-Path $PSScriptRoot 'Check-Merged.sql'))
    if ($diagnostic.Trim()) { throw "Merged diagnostic found issues: $diagnostic" }
    $checks.Add([ordered]@{ name = 'read-only merged diagnostic returns zero issues'; result = 'PASS' })
    Invoke-Job reverse check
    Invoke-Job reverse init
    Assert-Sql 'reverse snapshot splits to three tables' rollback @'
SELECT (SELECT quantity FROM inventory_rollback.mem_user_inventory WHERE uid=11)=10
 AND (SELECT quantity FROM inventory_rollback.mem_user_inventory_lock WHERE uid=11)=3
 AND (SELECT quantity FROM inventory_rollback.mem_user_inventory_tracking WHERE uid=11)=2
'@
    Assert-Sql 'reverse IDs use summary ID not original lock ID' rollback 'SELECT id=101 FROM inventory_rollback.mem_user_inventory_lock WHERE uid=11'

    # Intermediate on_hand=2, reserved=3, hold=2 is invalid; final tx state is valid.
    $null = Invoke-Sql mirror @'
BEGIN;
UPDATE inventory_mirror.mem_user_inventory SET quantity=2, last_update_time=1700000003 WHERE id=101;
UPDATE inventory_mirror.mem_user_inventory_lock SET quantity=0, last_update_time=1700000003 WHERE id=102;
UPDATE inventory_mirror.mem_user_inventory_tracking SET quantity=0, last_update_time=1700000003 WHERE id=103;
COMMIT;
'@
    Invoke-Job forward run
    Assert-Sql 'deferred projection validates final transaction state' merged 'SELECT (on_hand_qty,reserved_qty,hold_qty)=(2,0,0) FROM operations.inventory_summary WHERE id=101'
    Invoke-Job reverse run
    Assert-Sql 'forward-origin incremental events do not cascade to rollback' rollback 'SELECT quantity=10 FROM inventory_rollback.mem_user_inventory WHERE uid=11'
    $null = Invoke-Sql mirror @'
BEGIN;
DELETE FROM inventory_mirror.mem_user_inventory_lock WHERE id=102;
DELETE FROM inventory_mirror.mem_user_inventory_tracking WHERE id=103;
DELETE FROM inventory_mirror.mem_user_inventory WHERE id=101;
COMMIT;
'@
    Invoke-Job forward run
    Assert-Sql 'anchor deletion removes summary and outbox' merged 'SELECT NOT EXISTS(SELECT FROM operations.inventory_summary WHERE id=101) AND NOT EXISTS(SELECT FROM operations.inventory_summary_outbox WHERE summary_id=101)'
    Invoke-Job reverse run
    Assert-Sql 'forward-origin deletion also does not cascade' rollback 'SELECT count(*)=1 FROM inventory_rollback.mem_user_inventory WHERE uid=11'

    $null = Invoke-Sql merged @'
INSERT INTO operations.inventory_summary(id,warehouse_id,upc,goods_name,goods_version,owner_id,on_hand_qty,reserved_qty,hold_qty,updated_at)
 VALUES(201,1,'lab-upc','Review lab goods',1,22,20,4,2,to_timestamp(1700000010));
'@
    Invoke-Job reverse run
    Assert-Sql 'local confirmed insert fans out' rollback @'
SELECT (SELECT quantity FROM inventory_rollback.mem_user_inventory WHERE uid=22)=20
 AND (SELECT quantity FROM inventory_rollback.mem_user_inventory_lock WHERE uid=22)=4
 AND (SELECT quantity FROM inventory_rollback.mem_user_inventory_tracking WHERE uid=22)=2
'@
    # Demonstrate that changing only the target schema cannot adopt active mirror rows.
    $null = Invoke-Sql mirror "INSERT INTO inventory_mirror.mem_user_inventory(id,upc,uid,wh_id,quantity,last_update_time) VALUES(701,'lab-upc',22,'lab-wh',7,1700000010)"
    $mirrorConfig = (Get-Content -Raw -LiteralPath (Join-Path $inventoryRoot 'pgcat/summary-to-rollback-1ton.yaml')).
        Replace('inventory_summary_to_rollback_v1', 'inventory_summary_to_mirror_lab').
        Replace('inv_summary_to_rollback_v1', 'inv_summary_to_mirror_lab').
        Replace('INVENTORY_ROLLBACK_DSN', 'INVENTORY_MIRROR_DSN').
        Replace('inventory_rollback.', 'inventory_mirror.')
    $mirrorConfigPath = Join-Path $runRoot 'reverse-to-mirror.yaml'
    $mirrorConfig | Set-Content -LiteralPath $mirrorConfigPath -Encoding utf8
    $null = Invoke-Native $script:pgcat @('init', '--config', $mirrorConfigPath) 'unmarked existing row'
    Assert-Sql 'existing unmarked mirror row cannot be adopted by changing schema' mirror 'SELECT id=701 AND quantity=7 AND _sync_source IS NULL FROM inventory_mirror.mem_user_inventory WHERE uid=22'
    $checkpoint = (Invoke-Sql rollback "SELECT end_lsn FROM _pgcat_yaml.checkpoint WHERE job='inventory_summary_to_rollback_v1'").Trim()
    $null = Invoke-Sql rollback 'ALTER TABLE inventory_rollback.mem_user_inventory_lock ADD CONSTRAINT review_inject CHECK(quantity<=4)'
    $null = Invoke-Sql merged "UPDATE operations.inventory_summary SET on_hand_qty=21,reserved_qty=5,hold_qty=3,updated_at=to_timestamp(1700000011) WHERE id=201"
    Invoke-Job reverse run 'review_inject'
    Assert-Sql 'target failure leaves checkpoint unchanged' rollback "SELECT end_lsn FROM _pgcat_yaml.checkpoint WHERE job='inventory_summary_to_rollback_v1'" $checkpoint
    Assert-Sql 'target failure rolls back all three projections' rollback @'
SELECT (SELECT quantity FROM inventory_rollback.mem_user_inventory WHERE uid=22)=20
 AND (SELECT quantity FROM inventory_rollback.mem_user_inventory_lock WHERE uid=22)=4
 AND (SELECT quantity FROM inventory_rollback.mem_user_inventory_tracking WHERE uid=22)=2
'@
    $null = Invoke-Sql rollback 'ALTER TABLE inventory_rollback.mem_user_inventory_lock DROP CONSTRAINT review_inject'
    Invoke-Job reverse run
    Assert-Sql 'same config resumes failed WAL transaction' rollback @'
SELECT (SELECT quantity FROM inventory_rollback.mem_user_inventory WHERE uid=22)=21
 AND (SELECT quantity FROM inventory_rollback.mem_user_inventory_lock WHERE uid=22)=5
 AND (SELECT quantity FROM inventory_rollback.mem_user_inventory_tracking WHERE uid=22)=3
'@
    Invoke-Job reverse run
    Assert-Sql 'new process with no source changes causes no duplicate rows' rollback 'SELECT count(*)=1 FROM inventory_rollback.mem_user_inventory WHERE uid=22'
    $null = Invoke-Sql merged "UPDATE operations.inventory_summary SET on_hand_qty=21.5 WHERE id=201" 'inventory_summary_outbox_on_hand_qty_check'
    Assert-Sql 'legacy compatibility failure rolls back summary too' merged 'SELECT on_hand_qty=21 FROM operations.inventory_summary WHERE id=201'

    # BEFORE final constraints, explicitly flush deferred triggers inside subtransaction.
    $null = Invoke-Sql merged @'
DO $$
BEGIN
  BEGIN
    INSERT INTO inventory_stage.mem_user_inventory(id,upc,uid,wh_id,quantity,last_update_time)
      VALUES(301,'lab-upc',33,'lab-wh',1,1700000020);
    INSERT INTO inventory_stage.mem_user_inventory_lock(id,upc,uid,wh_id,quantity,last_update_time)
      VALUES(302,'lab-upc',33,'lab-wh',2,1700000020);
    SET CONSTRAINTS ALL IMMEDIATE;
    RAISE EXCEPTION 'expected invalid quantity rejection';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'on_hand(%) < reserved(%) + hold(%)%' THEN RAISE; END IF;
  END;
END $$;
'@
    Assert-Sql 'invalid final state rolls back stage summary and outbox' merged @'
SELECT NOT EXISTS(SELECT FROM inventory_stage.mem_user_inventory WHERE id=301)
 AND NOT EXISTS(SELECT FROM inventory_stage.mem_user_inventory_lock WHERE id=302)
 AND NOT EXISTS(SELECT FROM operations.inventory_summary WHERE id=301)
 AND NOT EXISTS(SELECT FROM operations.inventory_summary_outbox WHERE summary_id=301)
'@
    $null = Invoke-Sql merged @'
INSERT INTO operations.inventory_summary(id,warehouse_id,upc,goods_name,goods_version,owner_id,on_hand_qty)
 VALUES(401,1,'lab-upc','Review lab goods',1,NULL,1.5);
'@
    Assert-Sql 'TBA exists without entering legacy outbox' merged 'SELECT EXISTS(SELECT FROM operations.inventory_summary WHERE id=401 AND owner_id IS NULL) AND NOT EXISTS(SELECT FROM operations.inventory_summary_outbox WHERE summary_id=401)'
    Invoke-Job reverse run
    Assert-Sql 'TBA creates no rollback row' rollback 'SELECT count(*)=0 FROM inventory_rollback.mem_user_inventory WHERE id=401'
    $null = Invoke-Sql merged 'DELETE FROM operations.inventory_summary WHERE id=201'
    Invoke-Job reverse run
    Assert-Sql 'local confirmed deletion removes all three rollback rows' rollback @'
SELECT NOT EXISTS(SELECT FROM inventory_rollback.mem_user_inventory WHERE uid=22)
 AND NOT EXISTS(SELECT FROM inventory_rollback.mem_user_inventory_lock WHERE uid=22)
 AND NOT EXISTS(SELECT FROM inventory_rollback.mem_user_inventory_tracking WHERE uid=22)
'@
    # Build the existing engine tests against this isolated cluster, never a saved DSN.
    $env:PGCAT_TEST_DSN = $script:dsns.merged
    $env:PGCAT_TEST_ADMIN_DSN = $null
    Push-Location (Join-Path $repoRoot 'pgcat-yaml')
    try { $null = Invoke-Native $Go @('test', '-buildvcs=false', '-count=1', '-run', '^TestPostgresWALBidirectional$', '.') }
    finally { Pop-Location }
    $checks.Add([ordered]@{ name = 'existing pgcat real WAL bidirectional regression'; result = 'PASS' })
    $succeeded = $true
    Write-Host "PASS all $($checks.Count) review checks"
} catch {
    $failure = $_.Exception.Message
    throw
} finally {
    if ($started) {
        try { Invoke-PgCtl @('-D', $dataPath, '-m', 'fast', '-w', '-t', '30', 'stop') }
        catch { $succeeded = $false; $failure = "PostgreSQL stop failed: $($_.Exception.Message)"; Write-Warning $failure }
    }
    if (Test-Path -LiteralPath $runRoot) {
        $sourceFiles = @('pgcat-yaml/runtime.go','pgcat-yaml/engine.go','pgcat-yaml/store.go','pgcat-yaml/inventory_functions.go',
            'inventory-sync/sql/10_pg_merged.sql','inventory-sync/sql/20_pg_merged_projection.sql',
            'inventory-sync/pgcat/mirror-to-summary-n-to1.yaml','inventory-sync/pgcat/summary-to-rollback-1ton.yaml',
            'inventory-sync/review/Run-ReviewLab.ps1')
        $hashes = @($sourceFiles | ForEach-Object {
            [ordered]@{ file = $_; sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $repoRoot $_)).Hash }
        })
        [ordered]@{
            completed_at = [DateTimeOffset]::Now.ToString('o'); success = $succeeded
            postgres = $pgVersion; go = $goVersion; port = $Port
            scope = 'isolated PostgreSQL cluster with three databases; MySQL and Flink not executed'
            checks = @($checks.ToArray()); failure = $failure; source_hashes = $hashes
        } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $runRoot 'report.json') -Encoding utf8
        Write-Host "Report and stopped cluster retained at: $runRoot"
    }
    foreach ($name in $envNames) { [Environment]::SetEnvironmentVariable($name, $savedEnv[$name]) }
    if ($mappedDrive) { & subst "${mappedDrive}:" /D }
}
