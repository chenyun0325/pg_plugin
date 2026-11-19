param(
    [string]$Template = (Join-Path (Split-Path $PSScriptRoot -Parent) 'flink/inventory-mysql-to-pg-mirror-1to1.yaml'),
    [string]$FlinkCdc = 'flink-cdc.sh',
    [switch]$RenderOnly
)

$ErrorActionPreference = 'Stop'

$stringVariables = @(
    'MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD',
    'PG_HOST', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD'
)
$portVariables = @('MYSQL_PORT', 'PG_PORT')

$rendered = Get-Content -LiteralPath $Template -Raw
foreach ($name in $stringVariables) {
    $value = [Environment]::GetEnvironmentVariable($name)
    if ([string]::IsNullOrEmpty($value)) {
        throw "Environment variable $name is required"
    }
    if ($value.Contains("`r") -or $value.Contains("`n")) {
        throw "Environment variable $name contains a newline"
    }
    $rendered = $rendered.Replace("__${name}__", $value.Replace("'", "''"))
}
foreach ($name in $portVariables) {
    $value = [Environment]::GetEnvironmentVariable($name)
    if ($value -notmatch '^[0-9]+$') {
        throw "Environment variable $name must be an integer port"
    }
    $rendered = $rendered.Replace("__${name}__", $value)
}

$tempPath = Join-Path ([System.IO.Path]::GetTempPath()) ("inventory-flink-{0}.yaml" -f [guid]::NewGuid())
try {
    [System.IO.File]::WriteAllText($tempPath, $rendered, [System.Text.UTF8Encoding]::new($false))
    if ($RenderOnly) {
        Write-Output $tempPath
        return
    }
    & $FlinkCdc $tempPath
    if ($LASTEXITCODE -ne 0) {
        throw 'Flink CDC submission failed'
    }
}
finally {
    if (!$RenderOnly -and (Test-Path -LiteralPath $tempPath)) {
        Remove-Item -LiteralPath $tempPath -Force
    }
}
