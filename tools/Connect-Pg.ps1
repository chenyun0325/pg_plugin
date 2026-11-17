param(
    [string]$Database = 'postgres',
    [int]$LocalPort = 15432,
    [string[]]$PsqlArgs = @()
)
$ErrorActionPreference = 'Stop'
$pgClientExe = Join-Path $PSScriptRoot 'postgresql18\bin\psql.exe'
if (!(Test-Path -LiteralPath $pgClientExe)) { throw "psql client not found: $pgClientExe" }
# Password is prompted interactively and is not saved in this script.
& $pgClientExe -X -h 127.0.0.1 -p $LocalPort -U testuser -d $Database -W @PsqlArgs
exit $LASTEXITCODE
