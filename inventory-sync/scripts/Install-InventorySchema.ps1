param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('mirror', 'merged', 'rollback')]
    [string]$Role,

    [Parameter(Mandatory = $true)]
    [string]$Dsn,

    [string]$Psql = 'psql'
)

$ErrorActionPreference = 'Stop'
$scriptRoot = Split-Path $PSScriptRoot -Parent

$scripts = switch ($Role) {
    'mirror' {
        @('sql/00_pg_mirror.sql')
    }
    'merged' {
        @(
            'sql/10_pg_merged.sql',
            'sql/20_pg_merged_projection.sql',
            'sql/15_pg_flink_n_to1_adapter.sql'
        )
    }
    'rollback' {
        @('sql/30_pg_rollback.sql')
    }
}

foreach ($relativePath in $scripts) {
    $sqlPath = Join-Path $scriptRoot $relativePath
    & $Psql -X -v ON_ERROR_STOP=1 -d $Dsn -f $sqlPath
    if ($LASTEXITCODE -ne 0) {
        throw "psql failed for $sqlPath"
    }
}
