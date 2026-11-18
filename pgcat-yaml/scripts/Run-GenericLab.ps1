#Requires -Version 7.0
[CmdletBinding()]
param([string]$PgBin,[ValidateRange(1024,65535)][int]$Port=56489,[string]$Go='go')
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$false
$physicalRoot=Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$repoRoot=$physicalRoot
$relativeRun='tmp/generic-mapping-lab/'+[guid]::NewGuid().ToString('N')
$mappedDrive=$null;$started=$false;$success=$false;$failure=$null
$exeSuffix=if($IsWindows){'.exe'}else{''}
$savedEnv=@{}
foreach($name in @('PGCAT_TEST_DSN','PGCAT_TEST_ADMIN_DSN','GOCACHE','GOMODCACHE','PGCLIENTENCODING')){
 $savedEnv[$name]=[Environment]::GetEnvironmentVariable($name)
}
function Invoke-LabNative {
 param([string]$File,[string[]]$Arguments)
 $output=@(& $File @Arguments 2>&1 | ForEach-Object{"$_"});$code=$LASTEXITCODE
 $output | Add-Content -LiteralPath $script:logPath -Encoding utf8
 if($code -ne 0){throw "Command failed: $File ($code)`n$($output -join "`n")"}
 return ($output -join "`n")
}
function Invoke-LabControl {
 param([string[]]$Arguments)
 $out=Join-Path $script:runRoot 'pgctl.out.log';$err=Join-Path $script:runRoot 'pgctl.err.log'
 $parameters=@{FilePath=$script:pgCtl;ArgumentList=@($Arguments | ForEach-Object{'"'+$_.Replace('"','\"')+'"'});
  RedirectStandardOutput=$out;RedirectStandardError=$err;PassThru=$true}
 if($IsWindows){$parameters.WindowStyle='Hidden'}
 $process=Start-Process @parameters
 if(!$process.WaitForExit(45000)){throw 'pg_ctl did not finish within 45 seconds'}
 $result=(Get-Content -Raw -LiteralPath $out)+(Get-Content -Raw -LiteralPath $err)
 $result | Add-Content -LiteralPath $script:logPath -Encoding utf8
 if($process.ExitCode -ne 0){throw "pg_ctl failed: $result"}
}
try{
 if($IsWindows -and $repoRoot -match '[^\x00-\x7F]'){
  $used=@([IO.DriveInfo]::GetDrives() | ForEach-Object{$_.Name.Substring(0,1)})
  $drive=@('R','S','T','U','V','W','X','Y','Z' | Where-Object{$_ -notin $used}) | Select-Object -First 1
  if(!$drive){throw 'Unused ASCII drive alias required for Windows Unicode paths'}
  & subst "${drive}:" $physicalRoot
  if($LASTEXITCODE -ne 0){throw 'Cannot create temporary repository drive alias'}
  $mappedDrive=$drive;$repoRoot="${drive}:\"
  if($PgBin -and $PgBin.StartsWith($physicalRoot,[StringComparison]::OrdinalIgnoreCase)){
   $PgBin=Join-Path $repoRoot $PgBin.Substring($physicalRoot.Length).TrimStart('\','/')
  }
 }
 $runRoot=Join-Path $repoRoot $relativeRun;$dataPath=Join-Path $runRoot 'data';$logPath=Join-Path $runRoot 'commands.log'
 $null=New-Item -ItemType Directory -Force -Path $runRoot
 if(!$PgBin){
  $bundled=Join-Path $repoRoot 'tmp/toolchain/postgresql18-server/bin'
  if(Test-Path -LiteralPath (Join-Path $bundled "initdb$exeSuffix")){$PgBin=$bundled}
  else{$PgBin=Split-Path (Get-Command "initdb$exeSuffix" -ErrorAction Stop).Source -Parent}
 }
 $PgBin=(Resolve-Path -LiteralPath $PgBin).Path
 foreach($tool in @('initdb','postgres','pg_ctl','psql')){
  if(!(Test-Path -LiteralPath (Join-Path $PgBin "$tool$exeSuffix"))){throw "Missing server binary: $tool"}
 }
 $listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,$Port)
 try{$listener.Start()}finally{$listener.Stop()}
 $env:PGCLIENTENCODING='UTF8'
 if(!$env:GOCACHE){$env:GOCACHE=Join-Path $repoRoot 'tmp/gocache'}
 if(!$env:GOMODCACHE -and (Test-Path -LiteralPath (Join-Path $repoRoot 'tmp/gomodcache'))){$env:GOMODCACHE=Join-Path $repoRoot 'tmp/gomodcache'}
 $pgVersion=Invoke-LabNative (Join-Path $PgBin "postgres$exeSuffix") @('--version')
 $goVersion=Invoke-LabNative $Go @('version')
 Write-Host "Isolated generic mapping lab: $(Join-Path $physicalRoot $relativeRun)"
 $null=Invoke-LabNative (Join-Path $PgBin "initdb$exeSuffix") @('-D',$dataPath,'-U','mapping_lab','--auth-local=trust','--auth-host=trust','--encoding=UTF8','--locale=C')
 $pgCtl=Join-Path $PgBin "pg_ctl$exeSuffix"
 $options="-h 127.0.0.1 -p $Port -c wal_level=logical -c max_replication_slots=16 -c max_wal_senders=16"
 $started=$true
 Invoke-LabControl @('-D',$dataPath,'-l',(Join-Path $runRoot 'postgres.log'),'-o',$options,'-w','-t','30','start')
 $env:PGCAT_TEST_DSN="host=127.0.0.1 port=$Port user=mapping_lab dbname=postgres sslmode=disable connect_timeout=5"
 $env:PGCAT_TEST_ADMIN_DSN=$env:PGCAT_TEST_DSN
 Push-Location (Join-Path $repoRoot 'pgcat-yaml')
 try{
  $testOutput=Invoke-LabNative $Go @('test','-buildvcs=false','-count=1','-v','-timeout=20m','./...')
  $null=Invoke-LabNative $Go @('vet','./...')
  $binary=Join-Path $runRoot "pgcat-yaml-generic$exeSuffix"
  $null=Invoke-LabNative $Go @('build','-buildvcs=false','-o',$binary,'.')
  $null=Invoke-LabNative $binary @('convert-wms','--input','testdata/wms-generic-routes.yaml','--output',(Join-Path $runRoot 'wms-columns.yaml'))
 }finally{Pop-Location}
 $passCount=([regex]::Matches($testOutput,'(?m)^--- PASS:')).Count
 $skipCount=([regex]::Matches($testOutput,'(?m)^--- SKIP:')).Count
 if($skipCount -gt 0){throw "Integration regression unexpectedly skipped $skipCount tests"}
 $success=$true
 Write-Host "PASS $passCount top-level tests; no database tests skipped; vet/build/production-rule conversion passed"
}catch{$failure=$_.Exception.Message;throw}
finally{
 if($started){try{Invoke-LabControl @('-D',$dataPath,'-m','fast','-w','-t','30','stop')}catch{$success=$false;$failure="Stop failed: $($_.Exception.Message)";Write-Warning $failure}}
 if($runRoot -and (Test-Path -LiteralPath $runRoot)){
  $hashes=@(Get-ChildItem -LiteralPath (Join-Path $repoRoot 'pgcat-yaml') -Filter '*.go' | Sort-Object Name | ForEach-Object{
   [ordered]@{file="pgcat-yaml/$($_.Name)";sha256=(Get-FileHash -Algorithm SHA256 -LiteralPath $_.FullName).Hash}
  })
  [ordered]@{completed_at=[DateTimeOffset]::Now.ToString('o');success=$success;postgres=$pgVersion;go=$goVersion;
   tests_passed=$passCount;tests_skipped=$skipCount;port=$Port;
   scope='isolated PG WAL regression including original columns/sidecar/two-database tests; MySQL/Flink not executed';
   failure=$failure;source_hashes=$hashes} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $runRoot 'report.json') -Encoding utf8
  Write-Host "Report and stopped cluster: $(Join-Path $physicalRoot $relativeRun)"
 }
 foreach($name in $savedEnv.Keys){[Environment]::SetEnvironmentVariable($name,$savedEnv[$name])}
 if($mappedDrive){& subst "${mappedDrive}:" /D}
}
if(!$success){throw $failure}
