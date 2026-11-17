param(
    [int]$LocalPort = 15432,
    [string]$PrivateKey = (Join-Path $env:USERPROFILE 'Desktop\aws.pem')
)
$ErrorActionPreference = 'Stop'
if (!(Test-Path -LiteralPath $PrivateKey -PathType Leaf)) { throw "SSH key not found: $PrivateKey" }
if (Get-NetTCPConnection -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue) {
    throw "Local port $LocalPort is already listening; check the existing tunnel before starting another."
}
$pgTunnelLogDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'tmp\ssh'
New-Item -ItemType Directory -Path $pgTunnelLogDir -Force | Out-Null
$pgTunnelArgs = @(
    '-N', '-T',
    '-i', ('"' + $PrivateKey + '"'),
    '-p', '22',
    '-L', "127.0.0.1:${LocalPort}:172.31.31.245:31129",
    '-o', 'BatchMode=yes',
    '-o', 'ConnectTimeout=10',
    '-o', 'ExitOnForwardFailure=yes',
    '-o', 'StrictHostKeyChecking=accept-new',
    '-o', 'ServerAliveInterval=30',
    '-o', 'ServerAliveCountMax=3',
    'ec2-user@54.196.252.183'
)
$pgTunnelProcess = Start-Process -FilePath (Get-Command ssh.exe).Source `
    -ArgumentList $pgTunnelArgs -WindowStyle Hidden -PassThru `
    -RedirectStandardError (Join-Path $pgTunnelLogDir 'tunnel.stderr.log') `
    -RedirectStandardOutput (Join-Path $pgTunnelLogDir 'tunnel.stdout.log')
for ($pgAttempt = 0; $pgAttempt -lt 50; $pgAttempt++) {
    Start-Sleep -Milliseconds 250
    $pgTunnelProcess.Refresh()
    if ($pgTunnelProcess.HasExited) {
        throw (Get-Content -LiteralPath (Join-Path $pgTunnelLogDir 'tunnel.stderr.log') -Raw)
    }
    if (Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue) {
        Set-Content -LiteralPath (Join-Path $pgTunnelLogDir 'tunnel.pid') -Value $pgTunnelProcess.Id
        Write-Output "SSH tunnel PID=$($pgTunnelProcess.Id): 127.0.0.1:$LocalPort -> 172.31.31.245:31129 via 54.196.252.183"
        return
    }
}
throw 'SSH is still starting. Inspect tmp/ssh/tunnel.stderr.log and the SSH process before retrying.'
