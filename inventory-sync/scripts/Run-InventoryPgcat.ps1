param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('forward', 'reverse')]
    [string]$Direction,

    [Parameter(Mandatory = $true)]
    [ValidateSet('check', 'init', 'run')]
    [string]$Action,

    [string]$Pgcat = (Join-Path (Split-Path $PSScriptRoot -Parent) '../pgcat-yaml/bin/pgcat-yaml.exe'),
    [switch]$Once
)

$ErrorActionPreference = 'Stop'
$inventoryRoot = Split-Path $PSScriptRoot -Parent
$config = if ($Direction -eq 'forward') {
    Join-Path $inventoryRoot 'pgcat/mirror-to-summary-n-to1.yaml'
}
else {
    Join-Path $inventoryRoot 'pgcat/summary-to-rollback-1ton.yaml'
}

if (!(Test-Path -LiteralPath $Pgcat -PathType Leaf)) {
    throw "pgcat-yaml binary not found: $Pgcat"
}

if ($Direction -eq 'forward') {
    foreach ($name in @('INVENTORY_MIRROR_DSN', 'INVENTORY_MERGED_DSN')) {
        if ([string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($name))) {
            throw "Environment variable $name is required"
        }
    }
}
else {
    foreach ($name in @('INVENTORY_MERGED_DSN', 'INVENTORY_ROLLBACK_DSN')) {
        if ([string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($name))) {
            throw "Environment variable $name is required"
        }
    }
}

$arguments = @($Action, '--config', $config)
if ($Action -eq 'run' -and $Once) {
    $arguments += '--once'
}
& $Pgcat @arguments
if ($LASTEXITCODE -ne 0) {
    throw "pgcat-yaml $Action failed"
}
