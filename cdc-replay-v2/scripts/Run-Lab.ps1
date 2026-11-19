#Requires -Version 7.5
[CmdletBinding()]
param(
    [string]$Pgcat,
    [string]$PgFlo,
    [Parameter(Mandatory)][string]$PgSinkJar,
    [Parameter(Mandatory)][string]$MysqlSinkJar,
    [string]$Psql = 'psql',
    [string]$Mysql = 'mysql',
    [string]$Java = 'java',
    [string]$RunDirectory,
    [string]$FlinkClasspathFile
)
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$lab = Split-Path $PSScriptRoot -Parent
if (!$RunDirectory) {
    $RunDirectory = Join-Path ([IO.Path]::GetTempPath()) ('uid-cdc-' + [guid]::NewGuid().ToString('N'))
}
if (Test-Path -LiteralPath $RunDirectory) { throw 'Use a new directory for offsets and logs.' }
$run = (New-Item -ItemType Directory -Path $RunDirectory).FullName
if ($PgFlo) { $PgFlo=(Resolve-Path -LiteralPath $PgFlo).Path }
elseif ($Pgcat) { $Pgcat = (Resolve-Path -LiteralPath $Pgcat).Path }
else { throw 'Pass -Pgcat or -PgFlo' }
$PgSinkJar = (Resolve-Path -LiteralPath $PgSinkJar).Path
$MysqlSinkJar = (Resolve-Path -LiteralPath $MysqlSinkJar).Path
$Java = (Get-Command $Java).Source
$flinkClasspath = if ($FlinkClasspathFile) { (Get-Content -LiteralPath $FlinkClasspathFile -Raw).Trim() }
$workers = @{}
$dsns = @{
    UID_LAB_MIRROR_DSN = 'host=127.0.0.1 port=56543 user=postgres dbname=uid_mirror sslmode=disable'
    UID_LAB_MERGED_DSN = 'host=127.0.0.1 port=56543 user=postgres dbname=uid_merged sslmode=disable'
    UID_LAB_MYSQL_PASSWORD = ''
    UID_LAB_PG_PASSWORD = ''
}
$original = @{}
foreach ($key in $dsns.Keys) {
    $original[$key] = [Environment]::GetEnvironmentVariable($key)
    [Environment]::SetEnvironmentVariable($key, $dsns[$key])
}
function Native([string]$File, [string[]]$ArgList) {
    $lines = @(& $File @ArgList)
    if ($LASTEXITCODE -ne 0) { throw "$File failed ($LASTEXITCODE)" }
    return ($lines -join "`n").Trim()
}
function Pg([string]$Database, [string]$Sql) {
    Native $Psql @('-X','-h','127.0.0.1','-p','56543','-U','postgres','-d',$Database,
        '-A','-t','-q','-v','ON_ERROR_STOP=1','-c',$Sql)
}
function My([string]$Sql) {
    Native $Mysql @('--no-defaults','--protocol=TCP','-h','127.0.0.1','-P','53306','-u','root',
        '--batch','--skip-column-names','-e',$Sql)
}
function Wait-For([string]$Name, [scriptblock]$Probe, [string]$Expected, [int]$Timeout = 45) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    do {
        $actual = & $Probe
        if ("$actual" -eq $Expected) { return }
        Start-Sleep -Milliseconds 300
    } while ($watch.Elapsed.TotalSeconds -lt $Timeout)
    throw "$Name expected '$Expected', observed '$actual'. Inspect $run"
}
function Assert-Value([string]$Name, $Actual, $Expected) {
    if ("$Actual" -ne "$Expected") { throw "$Name expected '$Expected', observed '$Actual'." }
}
function Expect-Rejected([string]$Name, [scriptblock]$Write) {
    $rejected = $false
    try { $null = & $Write } catch { $rejected = $true }
    if (!$rejected) { throw "$Name unexpectedly succeeded." }
}
function Start-Worker([string]$Name, [string]$File, [string[]]$ArgList) {
    $argsQuoted = $ArgList | ForEach-Object { '"' + $_ + '"' }
    $params = @{FilePath=$File; ArgumentList=$argsQuoted; PassThru=$true; WorkingDirectory=$run;
        RedirectStandardOutput=(Join-Path $run "$Name.out"); RedirectStandardError=(Join-Path $run "$Name.err")}
    if ($IsWindows) { $params.WindowStyle = 'Hidden' }
    $workers[$Name] = Start-Process @params
}
function Stop-Worker([string]$Name) {
    $worker = $workers[$Name]
    if ($worker -and !$worker.HasExited) {
        Stop-Process -InputObject $worker
        $null = $worker.WaitForExit(10000)
    }
}
function Start-Bridge([string]$Name, [bool]$MysqlSource) {
    $jar = if ($MysqlSource) { $PgSinkJar } else { $MysqlSinkJar }
    $file = if ($MysqlSource) { 'mysql-to-mirror.yaml' } else { 'mirror-to-mysql.yaml' }
    $config = Join-Path $lab "config/$file"
    if ($flinkClasspath) {
        $class = if ($MysqlSource) { 'com.example.flinkcdc.tx.CdcTxJob' } else { 'com.example.flinkcdc.tx.mysql.MysqlTxJob' }
        return Start-Worker $Name $Java @('-cp',($jar + [IO.Path]::PathSeparator + $flinkClasspath),$class,'--config',$config)
    }
    Start-Worker $Name $Java @('-jar',$jar,'--config',$config)
}
function Start-MysqlSource {
    Start-Bridge mysql-to-mirror $true
    Wait-For 'MySQL binlog connected' {
        My "SELECT COUNT(*) FROM information_schema.processlist WHERE COMMAND LIKE 'Binlog Dump%'"
    } '1'
}
function Start-PgSource {
    Start-Bridge mirror-to-mysql $false
    Wait-For 'PG source slot active' { Pg postgres "SELECT count(*) FROM pg_replication_slots WHERE slot_name='inv_mysql_sink' AND active" } '1'
}
function MirrorStock {
    Pg uid_mirror "SELECT string_agg(qty::text,',' ORDER BY kind) FROM (
          SELECT 1 kind,qty FROM inventory_legacy.stock WHERE uid=1
          UNION ALL SELECT 2,qty FROM inventory_legacy.reserved WHERE uid=1
          UNION ALL SELECT 3,qty FROM inventory_legacy.hold WHERE uid=1
          UNION ALL SELECT 4,qty FROM inventory_legacy.available WHERE uid=1) t"
}
function MysqlStock {
    My "SELECT concat_ws(',',s.qty,r.qty,h.qty,a.qty) FROM inventory_legacy.stock s
        JOIN inventory_legacy.reserved r USING(uid) JOIN inventory_legacy.hold h USING(uid)
        JOIN inventory_legacy.available a USING(uid) WHERE s.uid=1"
}
function Summary([int]$Uid=1) {
    Pg uid_merged "SELECT concat_ws(',',stock_qty,reserved_qty,hold_qty,available_qty) FROM inventory_app.summary WHERE uid=$Uid"
}
function OwnershipSql([string]$SetClause, [string]$Guard) {
    "UPDATE cdc_meta.write_ownership SET $SetClause WHERE scope_namespace='inventory' AND uid=1 AND $Guard"
}
function Set-Ownership([string]$Node, [string]$SetClause, [string]$Guard) {
    $statement = OwnershipSql $SetClause $Guard
    if ($Node -eq 'mysql') { Assert-Value "CAS $Node" (My "$statement; SELECT ROW_COUNT();") 1 }
    else { Assert-Value "CAS $Node" (Pg "uid_$Node" "$statement RETURNING uid;") 1 }
}
function Drain([string]$Old, [int]$Epoch, [string]$Barrier) {
    if ($PgFlo) {
        & (Join-Path $lab '../pgflo-pg_flo-33adfb2/examples/uid-replay/Switch-Uid.ps1') -PgFlo $PgFlo -Psql $Psql -Mysql $Mysql -Writer $Old -Epoch $Epoch -Uid 1 -Barrier $Barrier
        return
    }
    $new = if ($Old -eq 'MYSQL') { 'PG' } else { 'MYSQL' }
    $writer = if ($Old -eq 'MYSQL') { 'mysql' } else { 'merged' }
    $nextWriter = if ($new -eq 'PG') { 'merged' } else { 'mysql' }
    $guard = "writer_side='$Old' AND epoch=$Epoch AND state='ACTIVE'"
    $stage = "state='DRAINING',barrier_id='$Barrier'"
    Set-Ownership $writer $stage $guard
    foreach ($node in @('mysql','mirror','merged') | Where-Object { $_ -ne $writer }) {
        Set-Ownership $node $stage $guard
    }
    $mark = "INSERT INTO cdc_meta.tx_context(marker_id,scope_namespace,root_tx_id,writer_side,apply_stage,uid,epoch,barrier_id)
        SELECT '$Barrier','inventory','$Barrier','$Old','native',uid,epoch,barrier_id
        FROM cdc_meta.write_ownership WHERE scope_namespace='inventory' AND uid=1 AND writer_side='$Old'
        AND epoch=$Epoch AND state='DRAINING' AND barrier_id='$Barrier'"
    if ($Old -eq 'MYSQL') {
        Assert-Value 'MySQL barrier insert' (My "$mark; SELECT ROW_COUNT();") 1
        Wait-For 'mirror forward barrier' {Pg uid_mirror "SELECT count(*) FROM cdc_meta.cutover_step WHERE barrier_id='$Barrier' AND channel='mysql_to_mirror' AND uid=1 AND epoch=$Epoch"} '1'
        Wait-For 'merged forward barrier' {Pg uid_merged "SELECT count(*) FROM cdc_meta.cutover_step WHERE barrier_id='$Barrier' AND channel='pgcat_forward' AND uid=1 AND epoch=$Epoch"} '1'
    } else {
        Assert-Value 'PG barrier insert' (Pg uid_merged "$mark RETURNING uid;") 1
        Wait-For 'mirror reverse barrier' {Pg uid_mirror "SELECT count(*) FROM cdc_meta.cutover_step WHERE barrier_id='$Barrier' AND channel='pgcat_reverse' AND uid=1 AND epoch=$Epoch"} '1'
        Wait-For 'MySQL reverse barrier' {My "SELECT count(*) FROM cdc_meta.cutover_step WHERE barrier_id='$Barrier' AND channel='mirror_to_mysql' AND uid=1 AND epoch=$Epoch"} '1'
    }
    $next = $Epoch + 1
    $activate = "writer_side='$new',epoch=$next,state='ACTIVE',barrier_id=NULL"
    $draining = "writer_side='$Old' AND epoch=$Epoch AND state='DRAINING' AND barrier_id='$Barrier'"
    foreach ($node in @('mysql','mirror','merged') | Where-Object { $_ -ne $nextWriter }) {
        Set-Ownership $node $activate $draining
    }
    Set-Ownership $nextWriter $activate $draining
}
Write-Host "Lab logs and offsets: $run"
try {
    Assert-Value 'Fresh PG databases' (Pg postgres "SELECT count(*) FROM pg_database WHERE datname IN ('uid_mirror','uid_merged')") 0
    Assert-Value 'Fresh MySQL databases' (My "SELECT count(*) FROM information_schema.schemata WHERE schema_name IN ('inventory_legacy','cdc_meta')") 0
    Assert-Value 'Row/full binlog' (My 'SELECT concat(@@log_bin,"/",@@binlog_format,"/",@@binlog_row_image)') '1/ROW/FULL'
    $null = My "SET GLOBAL time_zone='+00:00'"
    $mysqlSql = (Get-Content -Raw (Join-Path $lab 'sql/mysql-meta.sql')) + "`n" + (Get-Content -Raw (Join-Path $lab 'sql/mysql-fixture.sql'))
    $mysqlSql | & $Mysql --no-defaults --protocol=TCP -h 127.0.0.1 -P 53306 -u root
    if ($LASTEXITCODE) { throw 'Could not install MySQL fixtures.' }
    foreach ($node in @('mirror','merged')) {
        $null = Pg postgres "CREATE DATABASE uid_$node"
        $null = Native $Psql @('-X','-h','127.0.0.1','-p','56543','-U','postgres','-d',"uid_$node",'-v','ON_ERROR_STOP=1',
            '-f',(Join-Path $lab 'sql/postgres-meta.sql'),'-f',(Join-Path $lab "sql/$node-fixture.sql"))
    }
    foreach ($direction in @('forward','reverse')) {
        if ($PgFlo) {
            $config=Join-Path $lab "../pgflo-pg_flo-33adfb2/examples/uid-replay/$direction.yaml"
            $null=Native $PgFlo @('job','setup','--job',$config)
            Start-Worker "pgcat-$direction" $PgFlo @('job','run','--job',$config)
        } else {
            $config = Join-Path $lab "config/pgcat-$direction.yaml"
            $null = Native $Pgcat @('init','--config',$config)
            Start-Worker "pgcat-$direction" $Pgcat @('run','--config',$config)
        }
    }
    Start-PgSource
    Start-MysqlSource
    $otherMarker = "INSERT INTO cdc_meta.tx_context(marker_id,scope_namespace,root_tx_id,writer_side,apply_stage,uid,epoch)
        VALUES ('other-family','other_namespace','other-family','MYSQL','native',1,1)"
    $null = My $otherMarker
    $null = Pg uid_mirror $otherMarker
    $null = Pg uid_merged $otherMarker

    $null = My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,1,'tx-four-mysql');
        UPDATE inventory_legacy.stock SET qty=101 WHERE uid=1;
        UPDATE inventory_legacy.reserved SET qty=102 WHERE uid=1;
        UPDATE inventory_legacy.hold SET qty=103 WHERE uid=1;
        UPDATE inventory_legacy.available SET qty=104 WHERE uid=1; COMMIT"
    Wait-For 'four MySQL tables projected' { Summary } '101,102,103,104'
    Wait-For 'MySQL transaction echo receipt' { My "SELECT count(*) FROM cdc_meta.applied_tx WHERE result='ECHO'" } '1'
    Assert-Value 'PG echo created no raw replay' (Pg uid_mirror "SELECT count(*) FROM cdc_meta.tx_context WHERE root_tx_id='tx-four-mysql'") 1
    Assert-Value 'other namespace marker not forwarded' (Pg uid_mirror "SELECT count(*) FROM cdc_meta.tx_context WHERE root_tx_id='other-family'") 1
    Assert-Value 'UID 2 initial values' (Summary 2) '20,21,22,23'
    Write-Host 'PASS MySQL four-table forward and echo suppression'

    $null = My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,1,'tx-before-barrier');
        UPDATE inventory_legacy.stock SET qty=111 WHERE uid=1; COMMIT"
    Drain MYSQL 1 barrier-mysql-1
    Assert-Value 'old master transaction applied before barrier' (Summary) '111,102,103,104'
    Expect-Rejected 'old MySQL writer' { My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,1,'stale-mysql'); ROLLBACK" }
    $null = Pg uid_merged "BEGIN; SELECT cdc_meta.begin_pg_write('inventory',1,2,'tx-four-pg');
        UPDATE inventory_app.summary SET stock_qty=201,reserved_qty=202,hold_qty=203,available_qty=204 WHERE uid=1; COMMIT"
    Wait-For 'PG aggregate replayed to legacy' { MysqlStock } '201,202,203,204'
    Wait-For 'PG reverse echo receipt' { Pg uid_mirror "SELECT count(*) FROM cdc_meta.applied_tx WHERE result='ECHO'" } '1'
    Assert-Value 'PG raw four tables' (MirrorStock) '201,202,203,204'
    Assert-Value 'Original raw IDs retained' (My "SELECT concat_ws(',',s.id,r.id,h.id,a.id) FROM inventory_legacy.stock s
        JOIN inventory_legacy.reserved r USING(uid) JOIN inventory_legacy.hold h USING(uid)
        JOIN inventory_legacy.available a USING(uid) WHERE s.uid=1") '1,1,3,4'
    Write-Host 'PASS MYSQL epoch 1 -> PG epoch 2, PG reverse and echo suppression'

    $null = My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',2,1,'tx-uid2-replace');
        DELETE FROM inventory_legacy.available WHERE id=14;
        INSERT INTO inventory_legacy.available(id,uid,qty) VALUES(14,2,24); COMMIT"
    Wait-For 'other UID remains writable via MySQL' { Summary 2 } '20,21,22,24'
    Stop-Worker mirror-to-mysql
    Start-PgSource
    Drain PG 2 barrier-pg-2
    Expect-Rejected 'old PG writer' { Pg uid_merged "BEGIN; SELECT cdc_meta.begin_pg_write('inventory',1,2,'stale-pg'); ROLLBACK" }
    $null = My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,3,'tx-failback');
        UPDATE inventory_legacy.stock SET qty=301 WHERE uid=1; COMMIT"
    Wait-For 'MySQL writer resumed without reverting other fields' { Summary } '301,202,203,204'
    Write-Host 'PASS independent UID, DELETE/INSERT, PG epoch 2 -> MYSQL epoch 3'

    Stop-Worker mysql-to-mirror
    $offset = Join-Path $run 'mysql-offset.dat'
    $backup = Join-Path $run 'mysql-offset-before-failure.dat'
    Copy-Item -LiteralPath $offset -Destination $backup
    Start-MysqlSource
    $null = Pg uid_mirror 'ALTER TABLE inventory_legacy.reserved ADD CONSTRAINT test_tx_atomic CHECK(qty<2000)'
    $before = Pg uid_mirror 'SELECT count(*) FROM cdc_meta.applied_tx'
    $null = My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,3,'tx-failure-replay');
        UPDATE inventory_legacy.stock SET qty=401 WHERE uid=1;
        UPDATE inventory_legacy.reserved SET qty=2000 WHERE uid=1; COMMIT"
    Wait-For 'source job stopped on target constraint' { $workers['mysql-to-mirror'].HasExited } 'True'
    if (!(Select-String -Quiet -SimpleMatch 'test_tx_atomic' (Join-Path $run 'mysql-to-mirror.err'))) {
        throw 'Expected target constraint name in failing worker log.'
    }
    Copy-Item (Join-Path $run 'mysql-to-mirror.err') (Join-Path $run 'constraint-failure.err')
    Assert-Value 'first table rolled back' (MirrorStock) '301,202,203,204'
    Assert-Value 'failed transaction has no receipt' (Pg uid_mirror 'SELECT count(*) FROM cdc_meta.applied_tx') $before
    $null = Pg uid_mirror 'ALTER TABLE inventory_legacy.reserved DROP CONSTRAINT test_tx_atomic'
    Start-MysqlSource
    Wait-For 'source transaction replayed from persisted offset' { Summary } '401,2000,203,204'
    Assert-Value 'recovery applied once' (Pg uid_mirror "SELECT count(*) FROM cdc_meta.tx_context WHERE root_tx_id='tx-failure-replay'") 1
    Write-Host 'PASS atomic rollback and restart/replay after repair'

    Stop-Worker mysql-to-mirror
    Copy-Item -LiteralPath $backup -Destination $offset -Force
    Start-MysqlSource
    $null = My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,3,'tx-after-duplicate');
        UPDATE inventory_legacy.hold SET qty=211 WHERE uid=1;
        UPDATE inventory_legacy.hold SET qty=203 WHERE uid=1; COMMIT"
    Wait-For 'progressed past duplicate transaction' { Pg uid_mirror "SELECT count(*) FROM cdc_meta.tx_context WHERE root_tx_id='tx-after-duplicate'" } '1'
    Assert-Value 'duplicate source position did not issue DML' (Pg uid_mirror "SELECT count(*) FROM cdc_meta.tx_context WHERE root_tx_id='tx-failure-replay'") 1
    Write-Host 'PASS stale source offset replay deduplicated'

    $testEpoch=3
    if ($PgFlo) {
        Stop-Worker pgcat-forward
        $null=Pg uid_merged 'ALTER TABLE inventory_projection.parts ADD CONSTRAINT pgflo_atomic CHECK(qty<3000)'
        $null=My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,3,'tx-pgflo-failure');
            UPDATE inventory_legacy.stock SET qty=411 WHERE uid=1;
            UPDATE inventory_legacy.reserved SET qty=3000 WHERE uid=1; COMMIT"
        Wait-For 'upstream two-table transaction received' {MirrorStock} '411,3000,203,204'
        $forward=Join-Path $lab '../pgflo-pg_flo-33adfb2/examples/uid-replay/forward.yaml'
        Start-Worker pgcat-forward $PgFlo @('job','run','--job',$forward)
        Wait-For 'pg_flo stops at failing transaction' {$workers['pgcat-forward'].HasExited} 'True'
        if (!(Select-String -Quiet -SimpleMatch 'pgflo_atomic' (Join-Path $run 'pgcat-forward.err'))) {
            throw 'Expected pg_flo target constraint in failing worker log'
        }
        Copy-Item (Join-Path $run 'pgcat-forward.err') (Join-Path $run 'pgflo-constraint-failure.err')
        Assert-Value 'pg_flo first output rolled back' (Summary) '401,2000,203,204'
        Assert-Value 'pg_flo failed marker rolled back' (Pg uid_merged "SELECT count(*) FROM cdc_meta.tx_context WHERE root_tx_id='tx-pgflo-failure'") 0
        $null=My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,3,'tx-after-pgflo-failure');
            UPDATE inventory_legacy.hold SET qty=212 WHERE uid=1; COMMIT"
        Wait-For 'later transaction queued' {MirrorStock} '411,3000,212,204'
        $null=Pg uid_merged 'ALTER TABLE inventory_projection.parts DROP CONSTRAINT pgflo_atomic'
        Start-Worker pgcat-forward $PgFlo @('job','run','--job',$forward)
        Wait-For 'pg_flo repaired transaction then later transaction' {Summary} '411,3000,212,204'
        Assert-Value 'pg_flo recovered transaction applied once' (Pg uid_merged "SELECT count(*) FROM cdc_meta.tx_context WHERE root_tx_id='tx-pgflo-failure'") 1
        Write-Host 'PASS pg_flo atomic rollback, ordered NATS retry and worker restart'

        Drain MYSQL 3 barrier-mysql-3
        $null=Pg uid_merged "BEGIN; SELECT cdc_meta.begin_write('inventory',1,'PG',4,'tx-pg-second-switch');
            UPDATE inventory_app.summary SET stock_qty=601,reserved_qty=602,hold_qty=603,available_qty=604 WHERE uid=1; COMMIT"
        Wait-For 'second PG ownership activation' {MysqlStock} '601,602,603,604'
        Drain PG 4 barrier-pg-4
        # Retrying the identical barrier is idempotent; a new barrier at epoch 4
        # cannot be substituted for the recorded transition.
        Drain PG 4 barrier-pg-4
        $null=My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,5,'tx-mysql-second-switch');
            UPDATE inventory_legacy.stock SET qty=401 WHERE uid=1;
            UPDATE inventory_legacy.reserved SET qty=2000 WHERE uid=1;
            UPDATE inventory_legacy.hold SET qty=203 WHERE uid=1;
            UPDATE inventory_legacy.available SET qty=204 WHERE uid=1; COMMIT"
        Wait-For 'second MySQL ownership activation' {Summary} '401,2000,203,204'
        Assert-Value 'UID 2 independent after four cutovers' (Summary 2) '20,21,22,24'
        Assert-Value 'all four cutovers recorded' (Pg uid_merged "SELECT count(*) FROM pgflo_meta.cutover_history WHERE scope_namespace='inventory' AND uid=1") 4
        $testEpoch=5
        Write-Host 'PASS MYSQL1 -> PG2 -> MYSQL3 -> PG4 -> MYSQL5 and idempotent controller retry'
    }

    $null = My "START TRANSACTION; CALL cdc_meta.begin_mysql_write('inventory',1,$testEpoch,'tx-mixed-uid');
        UPDATE inventory_legacy.stock SET qty=501 WHERE uid=1;
        UPDATE inventory_legacy.stock SET qty=502 WHERE uid=2; COMMIT"
    Wait-For 'source stopped for mixed UID' { $workers['mysql-to-mirror'].HasExited } 'True'
    if (!(Select-String -Quiet -SimpleMatch 'Row UID differs' (Join-Path $run 'mysql-to-mirror.err'))) {
        throw 'Expected mixed UID rejection in failing worker log.'
    }
    Assert-Value 'mixed UID rolled back at destination' (MirrorStock) '401,2000,203,204'
    Assert-Value 'UID 2 in summary unchanged' (Summary 2) '20,21,22,24'
    foreach ($node in @('pgcat-forward','pgcat-reverse','mirror-to-mysql')) {
        Assert-Value "$node healthy" $workers[$node].HasExited $false
    }
    Write-Host 'PASS mixed UID fail-closed'

    $savedOffset = Join-Path $run 'mysql-offset-preserved.dat'
    Move-Item -LiteralPath $offset -Destination $savedOffset
    try {
        Start-Bridge mysql-missing-offset $true
        Wait-For 'source refused to start without offset' { $workers['mysql-missing-offset'].HasExited } 'True'
        if (!(Select-String -Quiet -SimpleMatch 'durable offset diverged' (Join-Path $run 'mysql-missing-offset.err'))) {
            throw 'Expected durable offset rejection in worker log.'
        }
    } finally {
        Move-Item -LiteralPath $savedOffset -Destination $offset
    }
    Write-Host 'PASS missing source offset fail-closed'

    Copy-Item -LiteralPath $offset -Destination $savedOffset
    try {
        [IO.File]::WriteAllBytes($offset, [byte[]]@(172,237,0,5,115))
        Start-Bridge mysql-corrupt-offset $true
        Wait-For 'source refused to start from corrupt offset' { $workers['mysql-corrupt-offset'].HasExited } 'True'
        if (!(Select-String -Quiet -SimpleMatch 'Invalid durable source offset' (Join-Path $run 'mysql-corrupt-offset.err'))) {
            throw 'Expected corrupt offset rejection in worker log.'
        }
    } finally {
        Move-Item -LiteralPath $savedOffset -Destination $offset -Force
    }
    Write-Host 'PASS truncated source offset fail-closed'
    'PASS' | Set-Content -LiteralPath (Join-Path $run 'result.txt')
} finally {
    foreach ($node in $workers.Keys) { Stop-Worker $node }
    foreach ($key in $original.Keys) { [Environment]::SetEnvironmentVariable($key,$original[$key]) }
}
