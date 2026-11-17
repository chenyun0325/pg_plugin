# PostgreSQL client and SSH tunnel

Installed client: PostgreSQL psql 18.7 (Windows x64), portable installation in `postgresql18/bin`.

Source: https://get.enterprisedb.com/postgresql/postgresql-18.7-1-windows-x64-binaries.zip

The extracted psql.exe Authenticode signature verified as Valid, EnterpriseDB Corporation.
Downloaded archive SHA256: `EAD0BBC27FB678FA24DCF86B738981323042ABD073C744103BF7F3AED23F9A33`.
This is a locally computed checksum for reproducibility, not an independently published checksum comparison.

Tunnel: `127.0.0.1:15432 -> 172.31.31.245:31129` via `ec2-user@54.196.252.183:22`.
Private key is read from the user's Desktop/aws.pem; its contents are not copied into the workspace.
New host keys are accepted on first use; changed known host keys are rejected by OpenSSH.

From the workspace root:

```powershell
# Start only if the tunnel is not already running.
.\tools\Start-PgTunnel.ps1
# Connect and enter the database password when prompted.
.\tools\Connect-Pg.ps1 -Database postgres
# After the target database name is confirmed:
.\tools\Connect-Pg.ps1 -Database testdb
```

Read-only connection check on 2026-11-16 succeeded against `postgres` as `testuser`:

- Server PostgreSQL 18.6, wal_level=logical.
- Non-template databases: postgres, testdb.
- User-requested testsdb returned `database does not exist`.
- No database objects were created or changed.

The tunnel runs as a background ssh process, not an auto-start Windows service.
Its PID and logs are under `../tmp/ssh/`. The startup script rejects an already occupied local port.
Inspect that PID before stopping it; do not stop unrelated SSH sessions.
Passwords are not stored in these scripts or this document.
