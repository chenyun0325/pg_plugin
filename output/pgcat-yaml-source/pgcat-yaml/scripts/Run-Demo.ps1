param([ValidateSet('A','B')][string]$Direction='A', [string]$Dsn='host=127.0.0.1 port=15432 dbname=mapping_poc user=testuser sslmode=disable')
$ErrorActionPreference='Stop'
$pgRoot=Split-Path $PSScriptRoot -Parent
$env:PGCAT_A_DSN=$Dsn
$env:PGCAT_B_DSN=$Dsn
$pgOldPassword=$env:PGPASSWORD
try {
    if (!$env:PGPASSWORD) {$env:PGPASSWORD=[System.Net.NetworkCredential]::new('',(Read-Host 'Database password' -AsSecureString)).Password}
    $pgConfig=if($Direction -eq 'A'){'a-to-b.yaml'}else{'b-to-a.yaml'}
    & (Join-Path $pgRoot 'bin\pgcat-yaml.exe') run --config (Join-Path $pgRoot ('examples\'+$pgConfig))
    exit $LASTEXITCODE
} finally {$env:PGPASSWORD=$pgOldPassword}
