#Requires -Version 7.5
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$PgFlo,
    [Parameter(Mandatory)][string]$Psql,
    [Parameter(Mandatory)][string]$Mysql,
    [Parameter(Mandatory)][ValidateSet('MYSQL','PG')][string]$Writer,
    [Parameter(Mandatory)][ValidateRange(1,9223372036854775806)][long]$Epoch,
    [Parameter(Mandatory)][ValidateRange(0,9223372036854775807)][long]$Uid,
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9_-]{1,64}$')][string]$Barrier,
    [ValidatePattern('^[A-Za-z0-9_-]{1,64}$')][string]$Namespace='inventory',
    [string]$ForwardJob=(Join-Path $PSScriptRoot 'forward.yaml'),
    [string]$ReverseJob=(Join-Path $PSScriptRoot 'reverse.yaml'),
    [string]$PgHost='127.0.0.1', [int]$PgPort=56543, [string]$PgUser='postgres',
    [string]$MirrorDatabase='uid_mirror', [string]$MergedDatabase='uid_merged',
    [string]$MysqlHost='127.0.0.1', [int]$MysqlPort=53306, [string]$MysqlUser='root',
    [int]$TimeoutSeconds=60
)
# Reference controller for the four-hop inventory lab. The controller must be
# trusted: it attests BOTH remote hop receipts before allowing local activation.
# Passwords/TLS settings come from PG*/MYSQL_PWD and the two reviewed job YAMLs.
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$false
function Native([string]$File,[string[]]$ArgList) {
    $lines=@(& $File @ArgList)
    if ($LASTEXITCODE) { throw "$File exited $LASTEXITCODE" }
    ($lines -join "`n").Trim()
}
function Quote([string]$Value) { "'"+$Value.Replace("'","''")+"'" }
function Sql([string]$Node,[string]$Statement) {
    if ($Node -eq 'mysql') {
        return Native $Mysql @('--no-defaults','--protocol=TCP','-h',$MysqlHost,'-P',"$MysqlPort",'-u',$MysqlUser,'--batch','--skip-column-names','-e',$Statement)
    }
    $database=if($Node -eq 'mirror'){$MirrorDatabase}else{$MergedDatabase}
    Native $Psql @('-X','-h',$PgHost,'-p',"$PgPort",'-U',$PgUser,'-d',$database,'-A','-t','-q','-v','ON_ERROR_STOP=1','-c',$Statement)
}
function Ownership([string]$Node,[string]$Action,[string[]]$Extra=@()) {
    $job=if($Node -eq 'mirror'){$ForwardJob}else{$ReverseJob}
    $null=Native $PgFlo (@('job','ownership','--job',$job,'--node','source','--action',$Action,
        '--uid',"$Uid",'--writer',$Writer,'--epoch',"$Epoch",'--barrier',$Barrier)+$Extra)
}
$scope=Quote $Namespace
$bar=Quote $Barrier
$nextWriter=if($Writer -eq 'MYSQL'){'PG'}else{'MYSQL'}
$nextEpoch=$Epoch+1
$oldNode=if($Writer -eq 'MYSQL'){'mysql'}else{'merged'}
$newNode=if($nextWriter -eq 'MYSQL'){'mysql'}else{'merged'}
$nodes=@('mysql','mirror','merged')
$oldActive="$Writer|$Epoch|ACTIVE|"
$draining="$Writer|$Epoch|DRAINING|$Barrier"
$nextActive="$nextWriter|$nextEpoch|ACTIVE|"
$stateSql="SELECT concat_ws('|',writer_side,epoch,state,coalesce(barrier_id,'')) FROM cdc_meta.write_ownership WHERE scope_namespace=$scope AND uid=$Uid"
$states=@{}
foreach($node in $nodes) {
    $states[$node]=Sql $node $stateSql
    if($states[$node] -notin @($oldActive,$draining,$nextActive)) { throw "Unexpected ownership at $node : $($states[$node])" }
}
if(@($nodes | Where-Object {$states[$_] -ne $nextActive}).Count -eq 0) {
    # Validate durable barrier evidence too: an externally forced epoch does not
    # count as an idempotent completion of this controller's barrier.
    foreach($node in @('mirror','merged')) {
        $n=Sql $node "SELECT count(*) FROM pgflo_meta.cutover_history WHERE scope_namespace=$scope AND uid=$Uid AND old_epoch=$Epoch AND barrier_id=$bar"
        if($n -ne '1') { throw "Missing cutover history at $node" }
    }
    Write-Host "Already active: uid=$Uid $nextWriter/$nextEpoch barrier=$Barrier"
    return
}
foreach($node in @($oldNode)+@($nodes | Where-Object {$_ -ne $oldNode})) {
    if($states[$node] -eq $nextActive) {continue}
    if($node -eq 'mysql') {
        $null=Sql $node "UPDATE cdc_meta.write_ownership SET state='DRAINING',barrier_id=$bar WHERE scope_namespace=$scope AND uid=$Uid AND writer_side='$Writer' AND epoch=$Epoch AND state='ACTIVE';"
        if((Sql $node $stateSql) -ne $draining) {throw 'MySQL freeze CAS failed'}
    } else {Ownership $node freeze}
}
if($states[$oldNode] -ne $nextActive) {
    if($oldNode -eq 'mysql') {
        $null=Sql mysql "INSERT INTO cdc_meta.tx_context(marker_id,scope_namespace,root_tx_id,writer_side,apply_stage,uid,epoch,barrier_id)
            SELECT $bar,$scope,$bar,'MYSQL','native',uid,epoch,barrier_id FROM cdc_meta.write_ownership
            WHERE scope_namespace=$scope AND uid=$Uid AND writer_side='MYSQL' AND epoch=$Epoch AND state='DRAINING' AND barrier_id=$bar
            AND NOT EXISTS(SELECT 1 FROM cdc_meta.tx_context WHERE marker_id=$bar)"
    } else {Ownership merged barrier}
}
$hops=if($Writer -eq 'MYSQL'){@(@('mirror','mysql_to_mirror'),@('merged','pgcat_forward'))}else{@(@('mirror','pgcat_reverse'),@('mysql','mirror_to_mysql'))}
$watch=[Diagnostics.Stopwatch]::StartNew()
$receipts=@()
foreach($hop in $hops) {
    $channel=Quote $hop[1]
    do {
        $position=Sql $hop[0] "SELECT source_position FROM cdc_meta.cutover_step WHERE barrier_id=$bar AND channel=$channel AND scope_namespace=$scope AND uid=$Uid AND epoch=$Epoch"
        if($position) {break}
        if($watch.Elapsed.TotalSeconds -ge $TimeoutSeconds) {throw "Barrier timeout at $($hop[1]); UID remains fenced. Retry with the SAME barrier."}
        Start-Sleep -Milliseconds 200
    }while($true)
    $receipts+=,@($hop[1],$position)
}
# Copy verified receipts as controller attestations; this does not create a CDC
# context marker, and metadata tables other than tx_context are not published.
foreach($node in $nodes) {
    foreach($receipt in $receipts) {
        $channel=Quote $receipt[0]; $position=Quote $receipt[1]
        $values="$bar,$channel,$scope,$Uid,$Epoch,$position"
        if($node -eq 'mysql') {$null=Sql $node "INSERT IGNORE INTO cdc_meta.cutover_step VALUES($values)"}
        else {$null=Sql $node "INSERT INTO cdc_meta.cutover_step VALUES($values) ON CONFLICT DO NOTHING"}
        $count=Sql $node "SELECT count(*) FROM cdc_meta.cutover_step WHERE barrier_id=$bar AND channel=$channel AND scope_namespace=$scope AND uid=$Uid AND epoch=$Epoch AND source_position=$position"
        if($count -ne '1') {throw "Receipt collision at $node"}
    }
}
$channels=($receipts | ForEach-Object {$_[0]}) -join ','
foreach($node in @($nodes | Where-Object {$_ -ne $newNode})+@($newNode)) {
    if($node -eq 'mysql') {
        $null=Sql mysql "UPDATE cdc_meta.write_ownership SET writer_side='$nextWriter',epoch=$nextEpoch,state='ACTIVE',barrier_id=NULL WHERE scope_namespace=$scope AND uid=$Uid AND writer_side='$Writer' AND epoch=$Epoch AND state='DRAINING' AND barrier_id=$bar"
    }else {Ownership $node activate @('--next-writer',$nextWriter,'--channels',$channels)}
    if((Sql $node $stateSql) -ne $nextActive) {throw "Activation CAS failed at $node"}
}
Write-Host "Activated uid=$Uid $Writer/$Epoch -> $nextWriter/$nextEpoch barrier=$Barrier"
