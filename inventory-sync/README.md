# Inventory synchronization deployment

Technical review and isolated reproduction: [库存同步技术评审](review/TECHNICAL_REVIEW.md).
Run [Run-ReviewLab.ps1](review/Run-ReviewLab.ps1) for a fresh local three-database
lab. See [validation](review/VALIDATION.md) for the verified scope. The deployment
goal is single business-writer ownership per uid, with MySQL and PG ownership
coexisting for different uids. Reverse output may reuse the PG mirror after uid
gating, cutover barriers and identity handoff are implemented. The current lab
still uses a separate rollback schema and does not implement PG-to-MySQL replay.

This directory implements the inventory topology as two explicit layers:

1. Flink CDC reads the three MySQL tables and writes three PostgreSQL mirror
   tables 1:1.
2. `pgcat-yaml` copies those mirror tables transactionally to three stage tables
   on the merged PostgreSQL node. Deferred PostgreSQL triggers calculate one
   `operations.inventory_summary` row per `(warehouse_id, upc, owner_id)`.
3. A plain outbox table represents confirmed summary rows. The reverse pgcat job
   fans each outbox row out to the three legacy-shaped rollback tables.

## Flink N:1 boundary

Apache Flink CDC 3.7 has no official PostgreSQL Pipeline sink. The workspace's
existing `type: postgres` is a custom sink. More importantly, Pipeline `route`
can route several tables to one sink table, but it does not perform a stateful
three-row join or column-wise partial upsert. Mapping absent quantities to zero
would let a lock event overwrite on-hand and hold values. A scalar transform UDF
does not make that operation transactionally correct.

Two correct alternatives are supplied:

- `flink/inventory-mysql-to-pg-component-n-to1.yaml` routes all three source
  schemas into one component table with a globally unique source-row key. SQL
  script `15_pg_flink_n_to1_adapter.sql` splits component state into the common
  stage and the deferred trigger builds the summary.
- `flink/inventory-mysql-to-pg-mirror-1to1.yaml` keeps a 1:1 PG mirror; the
  forward pgcat job copies it to the common stage and the same deferred trigger
  builds the summary.

These are mutually exclusive forward paths. Never run both against the same
merged stage tables.

## Field mapping

| Summary column | Source |
|---|---|
| `id` | `mem_user_inventory.id` (the anchor row) |
| `warehouse_id` | `inventory_warehouse_map(wh_id)` |
| `upc` | raw source `upc`, including surrounding spaces and the empty string; up to 255 characters without truncation |
| `goods_name`, `goods_version` | `inventory_goods_map(upc)`; missing mappings fall back to UPC and version 0 without modifying the map table |
| `owner_id` | `uid`; zero is retained as CONFIRMED, negative values are rejected |
| `on_hand_qty` | `mem_user_inventory.quantity` |
| `reserved_qty` | `mem_user_inventory_lock.quantity`, or zero |
| `hold_qty` | `mem_user_inventory_tracking.quantity`, or zero |
| `updated_at` | maximum source `last_update_time`, interpreted as Unix seconds |

`mem_user_inventory` is the anchor. A lock/tracking row without an inventory row
is an orphan and is rejected by preflight. A transient lock/hold row arriving
before its snapshot anchor creates no summary; the later anchor recomputes all
three quantities. Deleting the anchor deletes the
summary row. TBA rows are not synthesized because the legacy `uid` is NOT NULL;
locally-created TBA rows also do not enter the reverse legacy outbox.

Legacy `uid = 0` is retained as `owner_id = 0`, including zero-quantity rows.
It is CONFIRMED and enters the reverse outbox. Summary accepts nonnegative
owners or NULL; the reverse outbox accepts owners from 0 through 2147483647.
Existing deployments can apply `sql/22_pg_allow_zero_owner_migration.sql` and
run `sql/52_zero_owner_smoke_test.sql` (all test fixtures are rolled back).

Existing deployments apply `sql/28_pg_preserve_raw_inventory_keys_migration.sql`
to preserve UPC and warehouse keys without trimming. Restore previously trimmed
keys by exporting the source identities with `tools/InventoryRawKeyExport.java`
and running `scripts/Apply-RawInventoryKeys-With-ConnectPg.ps1`. Then apply
`sql/31_pg_allow_empty_upc_migration.sql` to retain MySQL's empty UPC values
through stage, summary and outbox. The raw-key and empty-UPC regression checks are
`sql/56_raw_inventory_keys_smoke_test.sql` and `sql/57_empty_upc_smoke_test.sql`;
their fixtures are rolled back. Environment-specific `*.local.yaml` files are
ignored by Git; render the committed templates with the environment variables
described below before submitting a job.

The custom PostgreSQL Flink sink must preserve all events from one MySQL
transaction in one PostgreSQL transaction whenever a business update changes
more than one of the three source tables. The deferred projection validates the
final target transaction state. If the sink splits such a transaction and an
intermediate state violates `on_hand >= reserved + hold`, projection stops
rather than publishing an invalid summary. Verify this connector property in a
failure/restart test before production rollout.

The current legacy import can temporarily retain negative available stock by
applying `sql/23_pg_relax_available_quantity_migration.sql`. This removes only
`ck_summary_available_nonnegative` and the corresponding projection-function
guard. Each quantity must still be nonnegative; the source quantities are also
retained in the reverse outbox. Monitor rows where
`on_hand_qty < reserved_qty + hold_qty` with the reconciliation query below.
After reconciling these rows, apply
`sql/24_pg_restore_available_quantity_check.sql` to restore both checks. Restore
fails without changing the schema/function if inconsistent rows remain. The
fresh-install schema remains strict; apply the temporary migration explicitly
after any migration that replaces the projection function (including SQL 21).

To also retain signed legacy quantities (for example tracking quantity -50),
apply `sql/25_pg_allow_negative_quantities_migration.sql` after SQL 23. This
removes the projection's negative-quantity exception and the three summary
quantity-nonnegative checks. Negative values are retained without clamping in
both summary and outbox. Owner, key, TBA and outbox integer/range constraints
remain active. Verify this mode with `sql/54_signed_quantities_smoke_test.sql`.
After reconciliation, SQL 26 restores only the nonnegative quantity checks;
SQL 24 separately restores the available-stock invariant. Existing restore
scripts reject inconsistent rows before committing any change. SQL 25 must be
reapplied if a later migration replaces the projection function with a strict
definition. The preflight report continues to list signed quantities for audit.

## Installation order

UPC compatibility: fresh installs use `varchar(255)` for summary, goods map and
outbox, matching the legacy MySQL columns and the existing stage/rollback
tables. For an existing 64-character deployment, apply
`sql/27_pg_upc_255_migration.sql` after approval. It widens these three columns
and patches only the current function's UPC guard and truncating cast, retaining
the previously chosen quantity/owner behavior. It uses a 5-second lock timeout
and rolls back the entire transaction if widening or function replacement fails.
Do not reapply the historical SQL 21 function definition afterwards without
reapplying SQL 27. No Pipeline YAML change is required for this widening.

Run `sql/55_long_upc_smoke_test.sql` after migration. The isolated pre-deployment
check is `./review/Test-Upc255Migration.ps1`: it reproduces the old 80-character
failure, checks distinct UPCs sharing the first 64 characters, ASCII/Unicode
255-character boundaries, zero owners, signed quantities and importer output.
It uses a fresh loopback-only database and does not consume deployment DSNs.
Goods import tools accept UPCs up to 255 characters; previous missing names
remain covered by the UPC/version-0 fallback until reference data is refreshed.

Set passwords through a DSN, `PGPASSFILE`, or the process environment. Do not
put credentials in the YAML files.

```powershell
# Mirror PostgreSQL
./scripts/Install-InventorySchema.ps1 -Role mirror -Dsn $env:INVENTORY_MIRROR_DSN

# Merged PostgreSQL
./scripts/Install-InventorySchema.ps1 -Role merged -Dsn $env:INVENTORY_MERGED_DSN

# Separate rollback PostgreSQL
./scripts/Install-InventorySchema.ps1 -Role rollback -Dsn $env:INVENTORY_ROLLBACK_DSN
```

Populate both mappings on the merged node before starting pgcat:

```sql
INSERT INTO operations.inventory_warehouse_map(legacy_wh_id, warehouse_id)
VALUES ('1', 1);

-- Replace this example with the authoritative goods master import.
INSERT INTO operations.inventory_goods_map(upc, goods_name, goods_version)
VALUES ('example-upc', 'Example goods', 0);
```

Run `sql/40_preflight.sql` on the mirror node after the Flink initial snapshot.
Every query must return zero rows.

After installing the merged schema, validate the projection SQL without leaving
test data behind:

```powershell
psql -X -v ON_ERROR_STOP=1 -d $env:INVENTORY_MERGED_DSN `
  -f ./sql/50_projection_smoke_test.sql
```

## Flink and pgcat startup

The Flink YAML uses placeholders so credentials are not stored in Git. Export
the nine variables named in `Render-And-Submit-Flink.ps1`, then submit:

```powershell
./scripts/Render-And-Submit-Flink.ps1 -FlinkCdc /opt/flink-cdc/bin/flink-cdc.sh
```

For the direct component N:1 alternative, pass its template explicitly:

```powershell
./scripts/Render-And-Submit-Flink.ps1 `
  -Template ./flink/inventory-mysql-to-pg-component-n-to1.yaml `
  -FlinkCdc /opt/flink-cdc/bin/flink-cdc.sh
```

Initialize the forward job only after reference mappings and preflight are
complete:

```powershell
./scripts/Run-InventoryPgcat.ps1 -Direction forward -Action check
./scripts/Run-InventoryPgcat.ps1 -Direction forward -Action init
./scripts/Run-InventoryPgcat.ps1 -Direction forward -Action run
```

Initialize the reverse job only when a separate rollback target is ready:

```powershell
./scripts/Run-InventoryPgcat.ps1 -Direction reverse -Action check
./scripts/Run-InventoryPgcat.ps1 -Direction reverse -Action init
./scripts/Run-InventoryPgcat.ps1 -Direction reverse -Action run
```

The current reverse YAML targets the isolated rollback schema. The intended
per-uid single-writer topology can instead reuse inventory_mirror; Flink may
continue serving other MySQL-owned uids. Before enabling that topology, implement
per-uid cutover barriers, ownership epochs, target identity handoff and the
PG-to-MySQL replay component. A schema rename alone does not implement these.
Replication origins suppress PG echo transactions; MySQL binlog replay also
needs ownership/provenance rules because PG origin is not preserved automatically.
See the technical review for the required acceptance scenarios.

Native `CREATE SUBSCRIPTION` can use `origin = none` for loop filtering on
PostgreSQL 16+, but it does not implement this three-table vertical field
projection. It can replace pgcat only for compatible 1:1 table replication.

## Operational checks

```sql
-- Merged result must agree with all three stage tables.
SELECT s.*
FROM operations.inventory_summary s
WHERE s.on_hand_qty < s.reserved_qty + s.hold_qty;

-- Monitor retained WAL for pgcat slots.
SELECT slot_name, active, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots
WHERE slot_name IN ('inv_mirror_to_summary_v1', 'inv_summary_to_rollback_v1');
```

The supplied DDL is intentionally fail-fast rather than idempotently accepting
an unknown existing schema. Review and version migrations for an existing
deployment instead of rerunning the create scripts.

## Upstream references

- [Flink CDC 3.7 connector matrix](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.7/docs/connectors/pipeline-connectors/overview/)
- [Flink CDC 3.7 route semantics](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.7/docs/core-concept/route/)
- [Flink CDC 3.7 MySQL source options](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.7/docs/connectors/pipeline-connectors/mysql/)
- [PostgreSQL 18 replication origins](https://www.postgresql.org/docs/18/replication-origins.html)
- [PostgreSQL 18 logical replication restrictions](https://www.postgresql.org/docs/18/logical-replication-restrictions.html)
