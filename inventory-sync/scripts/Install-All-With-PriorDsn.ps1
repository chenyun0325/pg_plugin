param(
    [string]$MirrorDsn = 'host=127.0.0.1 port=15432 dbname=testdb user=testuser sslmode=disable',
    [string]$MergedDsn = 'host=127.0.0.1 port=15432 dbname=testdb user=testuser sslmode=disable',
    [string]$RollbackDsn = 'host=127.0.0.1 port=15432 dbname=testdb user=testuser sslmode=disable'
)

$ErrorActionPreference = 'Stop'
$inventoryRoot = Split-Path $PSScriptRoot -Parent
$workspaceRoot = Split-Path $inventoryRoot -Parent
$installer = Join-Path $PSScriptRoot 'Install-InventorySchema.ps1'
$psql = Join-Path $workspaceRoot 'tools/postgresql18/bin/psql.exe'
$statusPath = Join-Path $workspaceRoot 'tmp/inventory-install-all.status'
$transcriptPath = Join-Path $workspaceRoot 'tmp/inventory-install-all.log'

$targets = @(
    @{ Role = 'mirror'; Dsn = $MirrorDsn; Marker = 'inventory_mirror.mem_user_inventory' },
    @{ Role = 'merged'; Dsn = $MergedDsn; Marker = 'operations.inventory_summary' },
    @{ Role = 'rollback'; Dsn = $RollbackDsn; Marker = 'inventory_rollback.mem_user_inventory' }
)

if (!(Test-Path -LiteralPath $installer -PathType Leaf)) {
    throw "Installer not found: $installer"
}
if (!(Test-Path -LiteralPath $psql -PathType Leaf)) {
    throw "psql not found: $psql"
}

$securePassword = Read-Host 'Password for PostgreSQL user testuser' -AsSecureString
$plainPassword = [System.Net.NetworkCredential]::new('', $securePassword).Password
$env:PGPASSWORD = $plainPassword
$env:INVENTORY_MIRROR_DSN = $MirrorDsn
$env:INVENTORY_MERGED_DSN = $MergedDsn
$env:INVENTORY_ROLLBACK_DSN = $RollbackDsn

Start-Transcript -LiteralPath $transcriptPath -Force | Out-Null
try {
    foreach ($target in $targets) {
        $marker = & $psql -X -w -At -v ON_ERROR_STOP=1 -d $target.Dsn `
            -c "SELECT to_regclass('$($target.Marker)')::text"
        if ($LASTEXITCODE -ne 0) {
            throw "Connection/object check failed for role $($target.Role)"
        }

        if ([string]::IsNullOrWhiteSpace(($marker | Out-String).Trim())) {
            Write-Host "Installing role $($target.Role)..."
            & $installer -Role $target.Role -Dsn $target.Dsn -Psql $psql
            if ($LASTEXITCODE -ne 0) {
                throw "Installation failed for role $($target.Role)"
            }
        }
        else {
            Write-Host "Role $($target.Role) already exists; skipping create."
        }

        $verified = & $psql -X -w -At -v ON_ERROR_STOP=1 -d $target.Dsn `
            -c "SELECT to_regclass('$($target.Marker)')::text"
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($verified | Out-String).Trim())) {
            throw "Verification failed for role $($target.Role)"
        }
        Write-Host "Role $($target.Role) verified: $($target.Marker)"
    }

    Set-Content -LiteralPath $statusPath -Value 'SUCCESS' -Encoding ascii
}
catch {
    Set-Content -LiteralPath $statusPath -Value ("FAILED: " + $_.Exception.Message) -Encoding utf8
    throw
}
finally {
    Stop-Transcript | Out-Null
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    $plainPassword = $null
    $securePassword.Dispose()
}
