param(
    [string]$Database = 'postgres',
    [int]$LocalPort = 15432,
    [string[]]$PsqlArgs = @(),
    [switch]$NoPasswordPrompt,
    [switch]$NoProcessExit
)
$ErrorActionPreference = 'Stop'
$pgClientExe = Join-Path $PSScriptRoot 'postgresql18\bin\psql.exe'
if (!(Test-Path -LiteralPath $pgClientExe)) { throw "psql client not found: $pgClientExe" }
# Password is prompted interactively unless the caller supplied it securely through
# libpq's standard mechanisms (for example, a process-scoped PGPASSWORD value).
$passwordMode = if ($NoPasswordPrompt) { '-w' } else { '-W' }
& $pgClientExe -X -h 127.0.0.1 -p $LocalPort -U testuser -d $Database $passwordMode @PsqlArgs
$pgExitCode = $LASTEXITCODE
if ($NoProcessExit) {
    return
}
exit $pgExitCode
