param(
    [string]$Database = 'testdb',
    [switch]$UseCredentialDialog
)

$ErrorActionPreference = 'Stop'
$workspaceRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$connectScript = Join-Path $workspaceRoot 'tools/Connect-Pg.ps1'
$sql = Join-Path $workspaceRoot 'tmp/inventory-goods-import.sql'
$statusPath = Join-Path $workspaceRoot 'tmp/inventory-goods-import.status'
$transcriptPath = Join-Path $workspaceRoot 'tmp/inventory-goods-import.log'

if (!(Test-Path -LiteralPath $sql)) {
    throw "Inventory goods import SQL not found: $sql"
}

$plainPassword = $null
if ($UseCredentialDialog) {
    Add-Type -AssemblyName System.Windows.Forms
    Add-Type -AssemblyName System.Drawing

    $form = New-Object System.Windows.Forms.Form
    $form.Text = 'PostgreSQL inventory import'
    $form.StartPosition = 'CenterScreen'
    $form.Size = New-Object System.Drawing.Size(430, 185)
    $form.TopMost = $true
    $form.FormBorderStyle = 'FixedDialog'
    $form.MaximizeBox = $false
    $form.MinimizeBox = $false

    $label = New-Object System.Windows.Forms.Label
    $label.Text = 'Password for PostgreSQL user testuser:'
    $label.AutoSize = $true
    $label.Location = New-Object System.Drawing.Point(18, 18)
    $form.Controls.Add($label)

    $passwordBox = New-Object System.Windows.Forms.TextBox
    $passwordBox.UseSystemPasswordChar = $true
    $passwordBox.Size = New-Object System.Drawing.Size(375, 24)
    $passwordBox.Location = New-Object System.Drawing.Point(18, 48)
    $form.Controls.Add($passwordBox)

    $okButton = New-Object System.Windows.Forms.Button
    $okButton.Text = 'Import'
    $okButton.DialogResult = [System.Windows.Forms.DialogResult]::OK
    $okButton.Location = New-Object System.Drawing.Point(233, 88)
    $form.Controls.Add($okButton)

    $cancelButton = New-Object System.Windows.Forms.Button
    $cancelButton.Text = 'Cancel'
    $cancelButton.DialogResult = [System.Windows.Forms.DialogResult]::Cancel
    $cancelButton.Location = New-Object System.Drawing.Point(318, 88)
    $form.Controls.Add($cancelButton)

    $form.AcceptButton = $okButton
    $form.CancelButton = $cancelButton
    $form.Add_Shown({ $passwordBox.Focus() })
    $dialogResult = $form.ShowDialog()
    if ($dialogResult -ne [System.Windows.Forms.DialogResult]::OK) {
        throw 'PostgreSQL password entry was cancelled.'
    }
    $plainPassword = $passwordBox.Text
    $passwordBox.Clear()
    $form.Dispose()
    if ([string]::IsNullOrEmpty($plainPassword)) {
        throw 'PostgreSQL password cannot be empty.'
    }
    $env:PGPASSWORD = $plainPassword
}

Start-Transcript -LiteralPath $transcriptPath -Force | Out-Null
try {
    & $connectScript -Database $Database -NoPasswordPrompt:$UseCredentialDialog -NoProcessExit -PsqlArgs @(
        '-v', 'ON_ERROR_STOP=1',
        '-P', 'pager=off',
        '-f', $sql
    )
}
finally {
    Stop-Transcript | Out-Null
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    $plainPassword = $null
}

$pgExitCode = $LASTEXITCODE
if ($pgExitCode -eq 0) {
    Set-Content -LiteralPath $statusPath -Value 'SUCCESS' -Encoding ascii
}
else {
    Set-Content -LiteralPath $statusPath -Value "FAILED:$pgExitCode" -Encoding ascii
}
exit $pgExitCode
