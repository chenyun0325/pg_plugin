param(
    [string]$Output = (Join-Path (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) 'tmp/inventory-goods-import.sql')
)

$ErrorActionPreference = 'Stop'
$inventoryRoot = Split-Path $PSScriptRoot -Parent
$workspaceRoot = Split-Path $inventoryRoot -Parent
$envFile = Join-Path $workspaceRoot 'env.txt'
$source = Join-Path $inventoryRoot 'tools/InventoryGoodsExport.java'
$classes = Join-Path $workspaceRoot 'tmp/inventory-goods-java'
$driver = Join-Path $env:USERPROFILE '.m2/repository/com/mysql/mysql-connector-j/9.1.0/mysql-connector-j-9.1.0.jar'

$configuration = Get-Content -LiteralPath $envFile -Raw
if ($configuration -notmatch '(?s)mysql库:\s*(?<user>[^@\s]+)@[^\r\n]*\s+password:\s*(?<password>\S+)') {
    throw 'Unable to read MySQL credentials from env.txt'
}

New-Item -ItemType Directory -Force -Path $classes | Out-Null
& javac -encoding UTF-8 -cp $driver -d $classes $source
if ($LASTEXITCODE -ne 0) {
    throw 'InventoryGoodsExport compilation failed'
}

$env:MYSQL_USER = $Matches.user
$env:MYSQL_PASSWORD = $Matches.password
try {
    & java -cp "$classes;$driver" InventoryGoodsExport $Output
    if ($LASTEXITCODE -ne 0) {
        throw 'Inventory goods export failed'
    }
}
finally {
    Remove-Item Env:MYSQL_USER -ErrorAction SilentlyContinue
    Remove-Item Env:MYSQL_PASSWORD -ErrorAction SilentlyContinue
    $configuration = $null
}
