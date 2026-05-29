# SigNoz ClickHouse: MergeTree → ReplicatedMergeTree migration

Operational runbook for `signoz_ch_replicate_migrate.py`.

Goal: take a single-node SigNoz ClickHouse install and convert its local
`MergeTree`-family tables to `Replicated*MergeTree`, then bring up a second
replica (`chs2`) so the cluster is **1 shard / 2 replicas** and SigNoz can run
with `SIGNOZ_OTEL_COLLECTOR_CLICKHOUSE_REPLICATION=true`.

Conversion method is the **shadow-table** path (create replicated shadow →
`ATTACH PARTITION FROM` original → verify → `RENAME` swap). We never use
`ALTER TABLE ... MODIFY ENGINE`.

---

## 0. Prerequisites

```bash
pip install clickhouse-connect
```

- Network access from where you run the script to **both** nodes on the HTTP
  port (`8123`, or `8443` with `--secure`).
- ClickHouse Keeper / ZooKeeper running and reachable from both nodes.
- `<macros>` configured: `chs1` = shard `01` / replica `chs1`, `chs2` = shard
  `01` / replica `chs2`. Restart ClickHouse after editing macros.
- A maintenance window. Writes must be stopped (phase 1) before phase 6.

---

## Global model — read this first

### Dry-run is the default. `--execute` is required to change anything.

```bash
# default = dry-run, makes NO changes (read-only queries + writes DDL preview files)
python3 signoz_ch_replicate_migrate.py --phase 6 --ch1-host chs1 --ch2-host chs2

# real run
python3 signoz_ch_replicate_migrate.py --phase 6 --ch1-host chs1 --ch2-host chs2 --execute
```

In dry-run, every mutating SQL is printed with a `-- [dry-run] not executed`
line and skipped. Read-only phases (2, 3, 4) do the same work either way.

### Where output goes

| Output | Location |
| --- | --- |
| Console log (every query + command) | stdout / errors to stderr |
| Inventory of all tables | `./signoz_ch_migration/inventory.json` (`--manifest`) |
| Generated DDL (reviewable before execute) | `./signoz_ch_migration/ddl/*.sql` (`--out-dir`) |

### Capture a log file for every run

Always `tee` so you have a record to revert from:

```bash
mkdir -p logs
python3 signoz_ch_replicate_migrate.py --phase 6 --ch1-host chs1 --ch2-host chs2 --execute \
  2>&1 | tee "logs/phase6_$(date +%Y%m%d_%H%M%S).log"
```

### Common flags

```
--ch1-host / --ch2-host     hostnames (default chs1 / chs2)
--port                      default 8123, or 8443 when --secure
--user / --password         CH credentials
--secure                    use HTTPS
--db-pattern                default 'signoz%'
--keeper-path-template      default /clickhouse/tables/{shard}/{database}/{table}
                            ({shard}/{replica} stay literal for CH macros)
--expected-shard            default 01
--ch1-replica/--ch2-replica default chs1 / chs2
--tmp-suffix                default __repl_tmp
--old-suffix                default __old_nonreplicated
--database / --table        restrict a phase to one DB / one table (great for phase 6 testing)
--dry-run / --execute       dry-run is on by default; --execute turns it off
```

### ClickHouse-side health queries (use these to check & verify any phase)

```sql
-- replica health (run on each node)
SELECT database, table, is_leader, total_replicas, active_replicas,
       queue_size, absolute_delay, readonly
FROM system.replicas
WHERE database LIKE 'signoz%'
ORDER BY database, table;

-- replication errors
SELECT database, table, type, num_tries, last_exception, create_time
FROM system.replication_queue
WHERE database LIKE 'signoz%'
ORDER BY create_time DESC
LIMIT 50;

-- in-flight cross-replica data copy (watch chs2 during phase 7 sync)
SELECT database, table, source_replica_hostname, progress, total_size_bytes_compressed
FROM system.replicated_fetches;

-- row / part parity (run on both nodes and compare)
SELECT database, table, sum(rows) AS rows,
       formatReadableSize(sum(bytes_on_disk)) AS size, count() AS parts
FROM system.parts
WHERE database LIKE 'signoz%' AND active
GROUP BY database, table
ORDER BY database, table;
```

ClickHouse server logs (when a command errors with no detail):
`/var/log/clickhouse-server/clickhouse-server.log` and `.err.log`, or
`docker logs <clickhouse-container>`.

### Recommended order

```
1 → 2 → 3 → 4 → 5 → 6 (test one table first) → 7 → 8 → 9
```
Each mutating phase: run dry-run, read the log + DDL files, then `--execute`.

---

## Phase 1 — Stop SigNoz collectors

**Purpose:** halt all inserts before touching schemas. Do not migrate while
data is being written.

**Dry run** (prints the command, runs nothing):
```bash
python3 signoz_ch_replicate_migrate.py --phase 1 \
  --stop-command "docker compose stop signoz-otel-collector signoz-otel-collector-metrics"
```

**Execute:**
```bash
python3 signoz_ch_replicate_migrate.py --phase 1 \
  --stop-command "docker compose stop signoz-otel-collector signoz-otel-collector-metrics" \
  --execute
```
Kubernetes example for `--stop-command`:
`"kubectl scale deploy/signoz-otel-collector --replicas=0 -n platform && kubectl scale deploy/signoz-otel-collector-metrics --replicas=0 -n platform"`

**Check / verify:** confirm no new rows are arriving:
```sql
SELECT max(timestamp) FROM signoz_logs.logs;     -- run twice, ~30s apart; should not advance
```

**Revert (restart collectors):**
```bash
docker compose start signoz-otel-collector signoz-otel-collector-metrics
# or: kubectl scale deploy/signoz-otel-collector --replicas=1 -n platform   (and -metrics)
```

---

## Phase 2 — Verify Keeper

**Purpose:** confirm ClickHouse Keeper/ZooKeeper is reachable from both nodes
and report `system.replicas`. **Read-only.**

```bash
python3 signoz_ch_replicate_migrate.py --phase 2 --ch1-host chs1 --ch2-host chs2
```

**Check logs for:** `[OK] Keeper visible from chs1 ...` and
`[OK] Keeper visible from chs2 ...`. If you see
`cannot read system.zookeeper -- ClickHouse Keeper/ZooKeeper is not configured`,
Keeper is down or not wired into `config.xml`. Fix `<zookeeper>` /
`<keeper_server>` and restart ClickHouse before continuing.

**Revert:** nothing — this phase makes no changes.

---

## Phase 3 — Verify macros

**Purpose:** assert `chs1` = shard `01`/replica `chs1` and `chs2` = shard
`01`/replica `chs2`. **Read-only.** Aborts on mismatch.

```bash
python3 signoz_ch_replicate_migrate.py --phase 3 --ch1-host chs1 --ch2-host chs2 \
  --expected-shard 01 --ch1-replica chs1 --ch2-replica chs2
```

**Check logs for:** `[OK] macros valid`. A `RuntimeError: chs2: expected
replica=chs2, got ...` means the macros are wrong — **do not proceed**, or both
nodes will try to register the same replica identity in Keeper.

**Fix (not a script step):** edit `<macros>` in the node's `config.xml`, then
`systemctl restart clickhouse-server` (or restart the container), re-run phase 3.

**Revert:** nothing — read-only.

---

## Phase 4 — Inventory

**Purpose:** list every `signoz%` table and classify it (local replicatable,
already replicated, distributed, view/MV, `.inner` MV-owned, migration
artifacts, skipped). Writes `inventory.json`. **Read-only on ClickHouse.**

```bash
python3 signoz_ch_replicate_migrate.py --phase 4 --ch1-host chs1 --ch2-host chs2
```

**Check:** open `signoz_ch_migration/inventory.json`. The
`local_replicatable` list is your phase-6 worklist. Confirm `internal_inner_tables`
is what you expect (these are handled via phase 9, not phase 6).

**Revert:** delete the JSON file if you want; harmless.

---

## Phase 5 — Create databases on chs2

**Purpose:** `CREATE DATABASE IF NOT EXISTS` for each `signoz%` DB on chs2.

**Dry run:**
```bash
python3 signoz_ch_replicate_migrate.py --phase 5 --ch1-host chs1 --ch2-host chs2
```
**Execute:**
```bash
python3 signoz_ch_replicate_migrate.py --phase 5 --ch1-host chs1 --ch2-host chs2 --execute
```

**Verify (on chs2):**
```sql
SELECT name FROM system.databases WHERE name LIKE 'signoz%' ORDER BY name;
```

**Revert (on chs2, only if empty / created in error):**
```sql
DROP DATABASE IF EXISTS signoz_traces;   -- repeat per DB; refuses nothing, so be sure it's empty
```

---

## Phase 6 — Convert local tables on chs1 (the critical phase)

**Purpose, per table:** create a `Replicated*` shadow `<table>__repl_tmp`,
`ATTACH PARTITION ... FROM` the original (non-destructive copy), verify row
parity, then `RENAME` swap: original → `<table>__old_nonreplicated`, shadow →
original. `.inner` MV tables and `__repl_tmp`/`__old_nonreplicated` artifacts
are skipped automatically.

> Merges are stopped on the source and shadow during the copy and restarted on
> the new table after the swap.

### 6a. Test ONE table first

```bash
# dry run a single table — writes the generated DDL, attaches nothing
python3 signoz_ch_replicate_migrate.py --phase 6 --ch1-host chs1 --ch2-host chs2 \
  --database signoz_traces --table signoz_index_v2
```
Review the generated DDL:
`signoz_ch_migration/ddl/signoz_traces.signoz_index_v2.phase6.create_tmp.sql`
— confirm engine, `ENGINE = Replicated...('/clickhouse/tables/{shard}/...','{replica}', <args>)`,
`PARTITION BY`, `ORDER BY`, `TTL`, `SETTINGS` all match the original.

```bash
# execute the single table
python3 signoz_ch_replicate_migrate.py --phase 6 --ch1-host chs1 --ch2-host chs2 \
  --database signoz_traces --table signoz_index_v2 --execute \
  2>&1 | tee "logs/phase6_index_v2_$(date +%Y%m%d_%H%M%S).log"
```

### 6b. Run the rest

```bash
python3 signoz_ch_replicate_migrate.py --phase 6 --ch1-host chs1 --ch2-host chs2          # dry run all
python3 signoz_ch_replicate_migrate.py --phase 6 --ch1-host chs1 --ch2-host chs2 --execute  # execute all
```

**Check logs / monitor a long copy (separate CH session):**
```sql
SELECT * FROM system.merges WHERE database LIKE 'signoz%';
SELECT database, table, sum(rows) rows, count() parts
FROM system.parts
WHERE database='signoz_traces' AND table IN ('signoz_index_v2','signoz_index_v2__repl_tmp') AND active
GROUP BY database, table;
```
The script aborts with `Row mismatch before rename ...` if source and shadow
row counts differ — in that case it leaves both tables in place for you to
inspect; nothing is swapped.

**Verify success (per table):**
```sql
SHOW TABLES FROM signoz_traces LIKE 'signoz_index_v2%';
-- expect: signoz_index_v2 (now Replicated) and signoz_index_v2__old_nonreplicated (old MergeTree)
SELECT engine FROM system.tables WHERE database='signoz_traces' AND name='signoz_index_v2';
SELECT count() FROM signoz_traces.signoz_index_v2;          -- matches the old count
SELECT count() FROM signoz_traces.signoz_index_v2__old_nonreplicated;
```

### Revert phase 6

Figure out which state the table is in, then apply the matching revert.
Run on **chs1**. Replace `{shard}` in Keeper paths with the real shard (`01`).

**State A — failure BEFORE the swap** (you see `signoz_index_v2` still as
MergeTree and a leftover `signoz_index_v2__repl_tmp`):
```sql
SYSTEM START MERGES signoz_traces.signoz_index_v2;          -- merges were stopped
DROP TABLE IF EXISTS signoz_traces.signoz_index_v2__repl_tmp;
-- if the dropped shadow leaves an orphan Keeper path (rare; "Replica already exists" on retry):
SYSTEM DROP REPLICA 'chs1' FROM ZKPATH '/clickhouse/tables/01/signoz_traces/signoz_index_v2';
```
The original table was never touched (ATTACH FROM only copies), so it is intact.

**State B — failure/rollback AFTER the swap** (`signoz_index_v2` is now
Replicated and `signoz_index_v2__old_nonreplicated` exists). Restore the old
MergeTree as the live table:
```sql
RENAME TABLE
    signoz_traces.signoz_index_v2 TO signoz_traces.signoz_index_v2__repl_tmp,
    signoz_traces.signoz_index_v2__old_nonreplicated TO signoz_traces.signoz_index_v2;
SYSTEM START MERGES signoz_traces.signoz_index_v2;
-- now discard the replicated copy and its Keeper path:
DROP TABLE IF EXISTS signoz_traces.signoz_index_v2__repl_tmp;
SYSTEM DROP REPLICA 'chs1' FROM ZKPATH '/clickhouse/tables/01/signoz_traces/signoz_index_v2';
```

> Keep `*__old_nonreplicated` tables until the whole migration is validated.
> Drop them only at the very end.

---

## Phase 7 — Create replicated tables on chs2

**Purpose:** replay each converted table's DDL on chs2. Because the engine uses
the same Keeper path with `{replica}` resolving to `chs2`, the new replica joins
the existing path and **Keeper streams all historical data from chs1**.

**Dry run / execute:**
```bash
python3 signoz_ch_replicate_migrate.py --phase 7 --ch1-host chs1 --ch2-host chs2
python3 signoz_ch_replicate_migrate.py --phase 7 --ch1-host chs1 --ch2-host chs2 --execute
```
Generated DDL: `signoz_ch_migration/ddl/*.phase7.create_chs2.sql`.

**Check / watch the sync (on chs2):**
```sql
SELECT * FROM system.replicated_fetches;                    -- data flowing chs1 -> chs2
SELECT database, table, total_replicas, active_replicas, queue_size, absolute_delay
FROM system.replicas WHERE database LIKE 'signoz%' ORDER BY absolute_delay DESC;
```
Sync is complete when `total_replicas = 2`, `active_replicas = 2`,
`queue_size → 0`, `absolute_delay → 0`. Then compare row/part parity (query in
the global section) between chs1 and chs2.

**Revert (on chs2):** dropping the table on chs2 removes only the chs2 replica
from Keeper; chs1 keeps its data.
```sql
DROP TABLE IF EXISTS signoz_traces.signoz_index_v2;         -- run on chs2 only
```

---

## Phase 8 — Recreate Distributed tables on chs2

**Purpose:** Distributed tables are routing-only (no data). Replay their DDL on
chs2 so both nodes can route queries/inserts.

```bash
python3 signoz_ch_replicate_migrate.py --phase 8 --ch1-host chs1 --ch2-host chs2
python3 signoz_ch_replicate_migrate.py --phase 8 --ch1-host chs1 --ch2-host chs2 --execute
```
Generated DDL: `signoz_ch_migration/ddl/*.phase8.create_distributed_chs2.sql`.

**Verify:** the cluster lists both replicas under one shard:
```sql
SELECT cluster, shard_num, replica_num, host_name FROM system.clusters
WHERE cluster NOT LIKE 'test%' ORDER BY cluster, shard_num, replica_num;
```

**Revert (on chs2):**
```sql
DROP TABLE IF EXISTS signoz_traces.distributed_signoz_index_v2;
```

---

## Phase 9 — Recreate Views / Materialized Views on chs2

**Purpose:** replay `View` / `MaterializedView` DDL on chs2. MVs are INSERT
triggers; both nodes need them so inserts landing on either replica populate the
(replicated) target tables. Run **after** phase 7 so target/source tables exist.

```bash
python3 signoz_ch_replicate_migrate.py --phase 9 --ch1-host chs1 --ch2-host chs2
python3 signoz_ch_replicate_migrate.py --phase 9 --ch1-host chs1 --ch2-host chs2 --execute
```
Generated DDL: `signoz_ch_migration/ddl/*.phase9.create_view_chs2.sql`.

**Verify (on chs2):**
```sql
SELECT name, engine FROM system.tables
WHERE database LIKE 'signoz%' AND engine IN ('View','MaterializedView')
ORDER BY database, name;
```

**Revert (on chs2):**
```sql
DROP VIEW IF EXISTS signoz_traces.<mv_name>;   -- or DROP TABLE for a MaterializedView
```

---

## After phase 9 — turn replication on (manual steps, not in the script)

1. Confirm health on both nodes: `system.replicas` shows `total_replicas=2`,
   `active_replicas=2`, `readonly=0`, `queue_size=0`, `absolute_delay=0`, and
   `system.replication_queue` has no `last_exception`.
2. Set in the SigNoz collector env and restart collectors:
   ```env
   SIGNOZ_OTEL_COLLECTOR_CLICKHOUSE_REPLICATION=true
   ```
   ```bash
   docker compose up -d signoz-otel-collector signoz-otel-collector-metrics
   ```
3. Validate fresh writes appear on **both** nodes:
   ```sql
   -- run on chs1 AND chs2
   SELECT count() FROM signoz_logs.logs WHERE timestamp >= now() - INTERVAL 5 MINUTE;
   ```
4. Confirm the SigNoz UI shows recent logs/traces/metrics.

---

## Full rollback to the pre-migration state

If you need to abandon the migration entirely (collectors should be stopped):

1. **chs2** — drop everything created there (views → distributed → replicated
   tables → databases):
   ```sql
   DROP DATABASE IF EXISTS signoz_traces;   -- repeat for signoz_logs / signoz_metrics
   ```
2. **chs1** — for every converted table, restore the old MergeTree using
   *State B* in phase 6's revert (rename `*__old_nonreplicated` back, drop the
   replicated copy, `SYSTEM DROP REPLICA ... FROM ZKPATH ...`, `SYSTEM START
   MERGES`).
3. Leave `SIGNOZ_OTEL_COLLECTOR_CLICKHOUSE_REPLICATION=false` and restart
   collectors (phase 1 revert).

---

## Troubleshooting

| Symptom | Likely cause / action |
| --- | --- |
| `cannot read system.zookeeper` (phase 2) | Keeper not configured/reachable. Fix `<zookeeper>`/`<keeper_server>`, restart CH. |
| `expected replica=chs2, got chs1` (phase 3) | Wrong macros on chs2 → would collide in Keeper. Fix `<macros>`, restart, re-run. |
| `Backup/Temp table already exists` (phase 6) | Leftover from a prior run. Inspect, then drop `*__repl_tmp` / handle `*__old_nonreplicated` before retrying. |
| `Row mismatch before rename` (phase 6) | Source ≠ shadow row count; nothing was swapped. Inspect parts; likely a part failed to attach. |
| `Replica already exists` when recreating a replicated table | Orphan Keeper path. `SYSTEM DROP REPLICA '<replica>' FROM ZKPATH '/clickhouse/tables/01/<db>/<table>'`, then retry. |
| chs2 stuck `active_replicas=1` after phase 7 | Watch `system.replicated_fetches` and `system.replication_queue.last_exception`; check network between nodes and disk space on chs2. |
| Merges seem stopped after an aborted phase 6 | `SYSTEM START MERGES <db>.<table>` on the affected table(s). |
