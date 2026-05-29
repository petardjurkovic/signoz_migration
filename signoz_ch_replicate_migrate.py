#!/usr/bin/env python3

import argparse
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Optional


REPLICATABLE_ENGINES = {
    "MergeTree": "ReplicatedMergeTree",
    "ReplacingMergeTree": "ReplicatedReplacingMergeTree",
    "SummingMergeTree": "ReplicatedSummingMergeTree",
    "AggregatingMergeTree": "ReplicatedAggregatingMergeTree",
    "CollapsingMergeTree": "ReplicatedCollapsingMergeTree",
    "VersionedCollapsingMergeTree": "ReplicatedVersionedCollapsingMergeTree",
}

REPLICATED_ENGINES = set(REPLICATABLE_ENGINES.values())

SKIP_ENGINES = {
    "Distributed",
    "View",
    "MaterializedView",
    "Null",
    "Buffer",
    "Kafka",
    "Memory",
    "Dictionary",
}

DEFAULT_SIGNOZ_DB_PATTERN = "signoz%"


@dataclass
class CHConfig:
    host: str
    port: int
    user: str
    password: str
    secure: bool = False
    connect_timeout: int = 10
    send_receive_timeout: int = 600


@dataclass
class TableInfo:
    database: str
    name: str
    engine: str
    engine_full: str


class CH:
    def __init__(self, name: str, cfg: CHConfig, dry_run: bool):
        import clickhouse_connect

        self.name = name
        self.cfg = cfg
        self.dry_run = dry_run
        self.client = clickhouse_connect.get_client(
            host=cfg.host,
            port=cfg.port,
            username=cfg.user,
            password=cfg.password,
            secure=cfg.secure,
            connect_timeout=cfg.connect_timeout,
            send_receive_timeout=cfg.send_receive_timeout,
        )

    def query_rows(self, sql: str, parameters: Optional[dict[str, Any]] = None) -> list[tuple]:
        print_sql(self.name, "QUERY", sql, parameters)
        result = self.client.query(sql, parameters=parameters or {})
        return list(result.result_rows)

    def query_one(self, sql: str, parameters: Optional[dict[str, Any]] = None) -> Optional[tuple]:
        rows = self.query_rows(sql, parameters)
        return rows[0] if rows else None

    def command(self, sql: str, parameters: Optional[dict[str, Any]] = None, force: bool = False):
        print_sql(self.name, "COMMAND", sql, parameters)
        if self.dry_run and not force:
            print("-- [dry-run] not executed")
            return None
        return self.client.command(sql, parameters=parameters or {})


def print_sql(node: str, kind: str, sql: str, parameters: Optional[dict[str, Any]] = None):
    print(f"\n[{node}] {kind}")
    print(sql.strip())
    if parameters:
        print(f"-- parameters: {json.dumps(parameters, default=str)}")


def qident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def qtable(database: str, table: str) -> str:
    return f"{qident(database)}.{qident(table)}"


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def render_keeper_path(template: str, database: str, table: str) -> str:
    # Only {database}/{table} are substituted here. {shard} and {replica} must
    # stay literal in the DDL so ClickHouse expands them per-node from <macros>.
    return template.replace("{database}", database).replace("{table}", table)


def save_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def connect(args) -> tuple["CH", "CH"]:
    common = {
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "secure": args.secure,
        "connect_timeout": args.connect_timeout,
        "send_receive_timeout": args.send_receive_timeout,
    }

    ch1 = CH("chs1", CHConfig(host=args.ch1_host, **common), dry_run=args.dry_run)
    ch2 = CH("chs2", CHConfig(host=args.ch2_host, **common), dry_run=args.dry_run)
    return ch1, ch2


def get_tables(ch: "CH", db_pattern: str) -> list[TableInfo]:
    rows = ch.query_rows(
        """
        SELECT
            database,
            name,
            engine,
            engine_full
        FROM system.tables
        WHERE database LIKE {db_pattern:String}
        ORDER BY database, name
        """,
        {"db_pattern": db_pattern},
    )
    return [TableInfo(database=r[0], name=r[1], engine=r[2], engine_full=r[3]) for r in rows]


def get_databases(ch: "CH", db_pattern: str) -> list[str]:
    rows = ch.query_rows(
        """
        SELECT name
        FROM system.databases
        WHERE name LIKE {db_pattern:String}
        ORDER BY name
        """,
        {"db_pattern": db_pattern},
    )
    return [r[0] for r in rows]


def show_create(ch: "CH", database: str, table: str) -> str:
    row = ch.query_one(f"SHOW CREATE TABLE {qtable(database, table)}")
    if not row:
        raise RuntimeError(f"SHOW CREATE returned no rows for {database}.{table}")
    return row[0]


def _scan_balanced_parens_end(ddl: str, open_index: int) -> int:
    """Given the index of a '(', return the index just past its matching ')'.

    Respects single-quoted strings and backtick-quoted identifiers so parens
    inside string literals or column comments do not throw off the depth count.
    """
    i = open_index
    n = len(ddl)
    depth = 0
    in_single = False
    in_backtick = False
    escape = False

    while i < n:
        c = ddl[i]
        if in_single:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == "'":
                in_single = False
        elif in_backtick:
            if c == "`":
                in_backtick = False
        else:
            if c == "'":
                in_single = True
            elif c == "`":
                in_backtick = True
            elif c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    return i + 1
        i += 1

    raise ValueError("Unbalanced parentheses while scanning DDL")


def find_engine_span(ddl: str) -> tuple[int, int, str, str]:
    """
    Returns (engine_expr_start, engine_expr_end, engine_name, engine_args).
    Searches for ENGINE only after the table's column list so that the word
    "ENGINE" appearing inside a column comment cannot be matched by mistake.
    """
    search_from = 0
    try:
        col_open = ddl.index("(")
        search_from = _scan_balanced_parens_end(ddl, col_open)
    except ValueError:
        search_from = 0

    chosen = None
    for m in re.finditer(r"\bENGINE\s*=\s*", ddl, flags=re.IGNORECASE):
        if m.start() >= search_from:
            chosen = m
            break
    if chosen is None:
        chosen = re.search(r"\bENGINE\s*=\s*", ddl, flags=re.IGNORECASE)
    if chosen is None:
        raise ValueError("DDL has no ENGINE clause")

    i = chosen.end()
    n = len(ddl)

    while i < n and ddl[i].isspace():
        i += 1

    name_start = i
    while i < n and (ddl[i].isalnum() or ddl[i] == "_"):
        i += 1

    engine_name = ddl[name_start:i]
    if not engine_name:
        raise ValueError("Could not parse engine name")

    name_end = i
    while i < n and ddl[i].isspace():
        i += 1

    args = ""
    # Default span ends right after the engine name so the whitespace/clauses
    # that follow (PARTITION BY, ORDER BY, ...) are preserved verbatim.
    expr_end = name_end

    if i < n and ddl[i] == "(":
        arg_start = i + 1
        expr_end = _scan_balanced_parens_end(ddl, i)
        args = ddl[arg_start:expr_end - 1]

    return chosen.start(), expr_end, engine_name, args.strip()


def replace_create_table_name(ddl: str, database: str, new_table: str) -> str:
    """
    Rewrites CREATE TABLE <old> ... into
    CREATE TABLE IF NOT EXISTS `db`.`new` (<column list and everything after>).
    Drops any UUID clause by chopping everything between the keyword and the
    first top-level '('. Intended for TABLE DDL only (not views).
    """
    m = re.search(r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?", ddl, flags=re.IGNORECASE)
    if not m:
        raise ValueError("DDL does not start with CREATE TABLE")

    try:
        col_open = ddl.index("(", m.end())
    except ValueError as exc:
        raise ValueError("Could not find column list in DDL") from exc

    return f"CREATE TABLE IF NOT EXISTS {qtable(database, new_table)} " + ddl[col_open:]


def to_replicated_create_ddl(
    original_ddl: str,
    database: str,
    original_table: str,
    new_table: str,
    keeper_path_template: str,
) -> str:
    ddl = replace_create_table_name(original_ddl, database, new_table)

    engine_start, engine_end, engine_name, engine_args = find_engine_span(ddl)

    if engine_name not in REPLICATABLE_ENGINES:
        raise ValueError(f"Engine {engine_name} is not supported for conversion")

    replicated_engine = REPLICATABLE_ENGINES[engine_name]
    keeper_path = render_keeper_path(keeper_path_template, database, original_table)

    if engine_args:
        new_engine_expr = (
            f"ENGINE = {replicated_engine}"
            f"({sql_string(keeper_path)}, '{{replica}}', {engine_args})"
        )
    else:
        new_engine_expr = (
            f"ENGINE = {replicated_engine}"
            f"({sql_string(keeper_path)}, '{{replica}}')"
        )

    return ddl[:engine_start] + new_engine_expr + ddl[engine_end:]


def make_chs2_ddl(ddl: str) -> str:
    """
    Make SHOW CREATE output safe to replay on chs2 for any object type
    (table, view, materialized view, dictionary, distributed):
      - strip the table-level UUID clause so chs2 assigns its own,
      - add IF NOT EXISTS,
      - otherwise preserve the statement verbatim (do NOT touch the column
        list or a view's SELECT body).
    """
    ddl = re.sub(r"(?is)\s+UUID\s+'[^']*'", " ", ddl, count=1)

    ddl = re.sub(
        r"(?is)^(\s*CREATE\s+(?:OR\s+REPLACE\s+)?"
        r"(?:MATERIALIZED\s+|LIVE\s+|WINDOW\s+)?"
        r"(?:TABLE|VIEW|DICTIONARY))\s+(?!IF\s+NOT\s+EXISTS\b)",
        r"\1 IF NOT EXISTS ",
        ddl,
        count=1,
    )
    return ddl.strip()


def is_internal_table(name: str) -> bool:
    # Hidden storage of materialized views; owned by the MV via UUID and must
    # never be renamed/converted directly.
    return name.startswith(".inner")


def is_migration_artifact(name: str, args) -> bool:
    return name.endswith(args.tmp_suffix) or name.endswith(args.old_suffix)


def table_exists(ch: "CH", database: str, table: str) -> bool:
    row = ch.query_one(
        """
        SELECT count()
        FROM system.tables
        WHERE database = {database:String}
          AND name = {table:String}
        """,
        {"database": database, "table": table},
    )
    return bool(row and int(row[0]) > 0)


def get_active_partition_ids(ch: "CH", database: str, table: str) -> list[str]:
    rows = ch.query_rows(
        """
        SELECT DISTINCT partition_id
        FROM system.parts
        WHERE database = {database:String}
          AND table = {table:String}
          AND active
        ORDER BY partition_id
        """,
        {"database": database, "table": table},
    )
    return [r[0] for r in rows]


def get_parts_summary(ch: "CH", database: str, tables: list[str]) -> dict[str, dict[str, Any]]:
    rows = ch.query_rows(
        """
        SELECT
            table,
            sum(rows) AS rows,
            sum(bytes_on_disk) AS bytes_on_disk,
            count() AS active_parts
        FROM system.parts
        WHERE database = {database:String}
          AND table IN {tables:Array(String)}
          AND active
        GROUP BY table
        ORDER BY table
        """,
        {"database": database, "tables": tables},
    )
    return {
        r[0]: {
            "rows": int(r[1]),
            "bytes_on_disk": int(r[2]),
            "active_parts": int(r[3]),
        }
        for r in rows
    }


def wait_for_replicas(ch: "CH", db_pattern: str, timeout_sec: int):
    deadline = time.time() + timeout_sec

    while time.time() < deadline:
        rows = ch.query_rows(
            """
            SELECT
                database,
                table,
                total_replicas,
                active_replicas,
                queue_size,
                absolute_delay,
                readonly
            FROM system.replicas
            WHERE database LIKE {db_pattern:String}
            ORDER BY database, table
            """,
            {"db_pattern": db_pattern},
        )

        bad = []
        for r in rows:
            _, _, total, active, _, _, readonly = r
            if int(total) < 2 or int(active) < 2 or int(readonly) != 0:
                bad.append(r)

        if not bad:
            print("[OK] replicas look healthy")
            return

        print("[WAIT] replicas not healthy yet:")
        for r in bad[:20]:
            print(r)

        time.sleep(5)

    raise TimeoutError("Timed out waiting for replicas")


def phase_1_stop_collectors(args):
    if not args.stop_command:
        raise SystemExit("--stop-command is required for phase 1")

    print(f"[phase 1] stop command: {args.stop_command}")

    if args.dry_run:
        print("[dry-run] command not executed")
        return

    if not args.execute:
        raise SystemExit("Refusing to execute. Add --execute.")

    subprocess.run(args.stop_command, shell=True, check=True)


def phase_2_verify_keeper(ch1: "CH", ch2: "CH"):
    for ch in (ch1, ch2):
        print(f"\n[phase 2] Keeper check on {ch.name}")

        try:
            rows = ch.query_rows(
                """
                SELECT name, value
                FROM system.zookeeper
                WHERE path = '/'
                ORDER BY name
                LIMIT 20
                """
            )
        except Exception as exc:
            raise RuntimeError(
                f"{ch.name}: cannot read system.zookeeper -- ClickHouse Keeper/ZooKeeper "
                f"is not configured or not reachable. Underlying error: {exc}"
            ) from exc

        print(f"[OK] Keeper visible from {ch.name}; root children={len(rows)}")

        replica_rows = ch.query_rows(
            """
            SELECT
                database,
                table,
                is_leader,
                total_replicas,
                active_replicas,
                queue_size,
                absolute_delay,
                readonly
            FROM system.replicas
            ORDER BY database, table
            LIMIT 20
            """
        )
        print(f"[INFO] system.replicas rows on {ch.name}: {len(replica_rows)}")


def phase_3_verify_macros(ch1: "CH", ch2: "CH", expected_shard: str, ch1_replica: str, ch2_replica: str):
    checks = [
        (ch1, expected_shard, ch1_replica),
        (ch2, expected_shard, ch2_replica),
    ]

    for ch, shard, replica in checks:
        print(f"\n[phase 3] macros on {ch.name}")

        rows = ch.query_rows(
            """
            SELECT macro, substitution
            FROM system.macros
            ORDER BY macro
            """
        )
        macros = {r[0]: r[1] for r in rows}
        print(json.dumps(macros, indent=2))

        if macros.get("shard") != shard:
            raise RuntimeError(f"{ch.name}: expected shard={shard}, got {macros.get('shard')}")
        if macros.get("replica") != replica:
            raise RuntimeError(f"{ch.name}: expected replica={replica}, got {macros.get('replica')}")

    print("[OK] macros valid")


def phase_4_inventory(ch1: "CH", args):
    tables = get_tables(ch1, args.db_pattern)

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "db_pattern": args.db_pattern,
        "tables": [asdict(t) for t in tables],
        "local_replicatable": [
            asdict(t)
            for t in tables
            if t.engine in REPLICATABLE_ENGINES
            and not is_internal_table(t.name)
            and not is_migration_artifact(t.name, args)
        ],
        "already_replicated": [asdict(t) for t in tables if t.engine in REPLICATED_ENGINES],
        "distributed": [asdict(t) for t in tables if t.engine == "Distributed"],
        "views": [asdict(t) for t in tables if t.engine in ("View", "MaterializedView")],
        "internal_inner_tables": [asdict(t) for t in tables if is_internal_table(t.name)],
        "migration_artifacts": [asdict(t) for t in tables if is_migration_artifact(t.name, args)],
        "skipped": [
            asdict(t)
            for t in tables
            if t.engine not in REPLICATABLE_ENGINES
            and t.engine not in REPLICATED_ENGINES
            and t.engine not in ("Distributed", "View", "MaterializedView")
        ],
    }

    save_json(args.manifest, payload)

    print(f"[OK] inventory saved: {args.manifest}")
    print(f"total tables: {len(payload['tables'])}")
    print(f"local replicatable: {len(payload['local_replicatable'])}")
    print(f"already replicated: {len(payload['already_replicated'])}")
    print(f"distributed: {len(payload['distributed'])}")
    print(f"views/materialized views: {len(payload['views'])}")
    print(f"internal .inner tables (handled via their MV): {len(payload['internal_inner_tables'])}")
    print(f"migration artifacts (*__repl_tmp / *__old): {len(payload['migration_artifacts'])}")
    print(f"skipped: {len(payload['skipped'])}")


def phase_5_create_databases(ch1: "CH", ch2: "CH", args):
    dbs = get_databases(ch1, args.db_pattern)
    for db in dbs:
        ch2.command(f"CREATE DATABASE IF NOT EXISTS {qident(db)}")
    print(f"[OK] databases processed on chs2: {len(dbs)}")


def should_process_table(args, t: TableInfo) -> bool:
    if args.database and t.database != args.database:
        return False
    if args.table and t.name != args.table:
        return False
    return True


def phase_6_migrate_local_tables_on_chs1(ch1: "CH", args):
    tables = get_tables(ch1, args.db_pattern)
    candidates = [
        t
        for t in tables
        if t.engine in REPLICATABLE_ENGINES
        and not is_internal_table(t.name)
        and not is_migration_artifact(t.name, args)
        and should_process_table(args, t)
    ]

    skipped_inner = [
        t for t in tables
        if t.engine in REPLICATABLE_ENGINES and is_internal_table(t.name) and should_process_table(args, t)
    ]
    for t in skipped_inner:
        print(
            f"[SKIP] {t.database}.{t.name} is a materialized-view .inner table; "
            f"it is migrated by recreating its owning MV (phase 9), not directly."
        )

    if not candidates:
        print("[INFO] no local MergeTree-family tables to migrate")
        return

    for t in candidates:
        db = t.database
        table = t.name
        tmp = f"{table}{args.tmp_suffix}"
        old = f"{table}{args.old_suffix}"

        print(f"\n[phase 6] migrating {db}.{table}: {t.engine} -> {REPLICATABLE_ENGINES[t.engine]}")

        if table_exists(ch1, db, old):
            raise RuntimeError(f"Backup table already exists: {db}.{old}. Resolve before continuing.")
        if table_exists(ch1, db, tmp):
            raise RuntimeError(f"Temp table already exists: {db}.{tmp}. Resolve before continuing.")

        original_ddl = show_create(ch1, db, table)
        replicated_ddl = to_replicated_create_ddl(
            original_ddl=original_ddl,
            database=db,
            original_table=table,
            new_table=tmp,
            keeper_path_template=args.keeper_path_template,
        )

        ddl_file = args.out_dir / "ddl" / f"{db}.{table}.phase6.create_tmp.sql"
        ddl_file.parent.mkdir(parents=True, exist_ok=True)
        ddl_file.write_text(replicated_ddl + ";\n", encoding="utf-8")
        print(f"[DDL] wrote {ddl_file}")

        # Freeze the source part set, create the replicated shadow, then copy
        # parts into it. ATTACH PARTITION FROM hard-links/copies parts, leaving
        # the source intact, which is what makes the later swap reversible.
        ch1.command(f"SYSTEM STOP MERGES {qtable(db, table)}")
        ch1.command(replicated_ddl)
        ch1.command(f"SYSTEM STOP MERGES {qtable(db, tmp)}")

        partition_ids = get_active_partition_ids(ch1, db, table)
        print(f"[INFO] active partitions to attach: {len(partition_ids)}")

        for partition_id in partition_ids:
            ch1.command(
                f"""
                ALTER TABLE {qtable(db, tmp)}
                ATTACH PARTITION ID {sql_string(partition_id)}
                FROM {qtable(db, table)}
                """
            )

        if args.dry_run or not args.execute:
            # Nothing was actually created/attached, so a row-count comparison
            # would be meaningless here. Just report the plan and move on.
            print(
                f"[DRY-RUN] would create {db}.{tmp}, attach {len(partition_ids)} "
                f"partition(s), verify row counts, then RENAME swap into {db}.{table}. "
                f"Re-run with --execute to perform it."
            )
            ch1.command(f"SYSTEM START MERGES {qtable(db, table)}")
            continue

        summary = get_parts_summary(ch1, db, [table, tmp])
        print(json.dumps(summary, indent=2))

        src_rows = summary.get(table, {}).get("rows", 0)
        tmp_rows = summary.get(tmp, {}).get("rows", 0)
        if src_rows != tmp_rows:
            raise RuntimeError(
                f"Row mismatch before rename for {db}.{table}: source={src_rows}, tmp={tmp_rows}. "
                f"Leaving tables in place for inspection."
            )

        ch1.command(
            f"""
            RENAME TABLE
                {qtable(db, table)} TO {qtable(db, old)},
                {qtable(db, tmp)} TO {qtable(db, table)}
            """
        )
        ch1.command(f"SYSTEM START MERGES {qtable(db, table)}")
        print(f"[OK] migrated {db}.{table}; old table kept as {db}.{old}")


def phase_7_create_replicated_tables_on_chs2(ch1: "CH", ch2: "CH", args):
    tables = get_tables(ch1, args.db_pattern)
    candidates = [
        t
        for t in tables
        if t.engine in REPLICATED_ENGINES
        and not is_migration_artifact(t.name, args)
        and not is_internal_table(t.name)
        and should_process_table(args, t)
    ]

    for t in candidates:
        db, table = t.database, t.name
        print(f"\n[phase 7] create replicated table on chs2: {db}.{table}")

        if table_exists(ch2, db, table):
            print(f"[SKIP] exists on chs2: {db}.{table}")
            continue

        ddl = make_chs2_ddl(show_create(ch1, db, table))

        ddl_file = args.out_dir / "ddl" / f"{db}.{table}.phase7.create_chs2.sql"
        ddl_file.parent.mkdir(parents=True, exist_ok=True)
        ddl_file.write_text(ddl + ";\n", encoding="utf-8")
        print(f"[DDL] wrote {ddl_file}")

        ch2.command(ddl)

    print("[OK] phase 7 done")


def phase_8_recreate_distributed_tables(ch1: "CH", ch2: "CH", args):
    tables = get_tables(ch1, args.db_pattern)
    candidates = [
        t for t in tables
        if t.engine == "Distributed" and should_process_table(args, t)
    ]

    for t in candidates:
        db, table = t.database, t.name
        print(f"\n[phase 8] create distributed table on chs2: {db}.{table}")

        if table_exists(ch2, db, table):
            print(f"[SKIP] exists on chs2: {db}.{table}")
            continue

        ddl = make_chs2_ddl(show_create(ch1, db, table))

        ddl_file = args.out_dir / "ddl" / f"{db}.{table}.phase8.create_distributed_chs2.sql"
        ddl_file.parent.mkdir(parents=True, exist_ok=True)
        ddl_file.write_text(ddl + ";\n", encoding="utf-8")
        print(f"[DDL] wrote {ddl_file}")

        ch2.command(ddl)

    print("[OK] phase 8 done")


def phase_9_recreate_views(ch1: "CH", ch2: "CH", args):
    tables = get_tables(ch1, args.db_pattern)
    candidates = [
        t for t in tables
        if t.engine in ("View", "MaterializedView") and should_process_table(args, t)
    ]

    for t in candidates:
        db, table = t.database, t.name
        print(f"\n[phase 9] create view on chs2: {db}.{table}")

        if table_exists(ch2, db, table):
            print(f"[SKIP] exists on chs2: {db}.{table}")
            continue

        ddl = make_chs2_ddl(show_create(ch1, db, table))

        ddl_file = args.out_dir / "ddl" / f"{db}.{table}.phase9.create_view_chs2.sql"
        ddl_file.parent.mkdir(parents=True, exist_ok=True)
        ddl_file.write_text(ddl + ";\n", encoding="utf-8")
        print(f"[DDL] wrote {ddl_file}")

        ch2.command(ddl)

    print("[OK] phase 9 done")


def parse_args():
    p = argparse.ArgumentParser(
        description="Manual SigNoz ClickHouse MergeTree -> ReplicatedMergeTree migration helper"
    )

    p.add_argument("--phase", type=int, required=True, choices=range(1, 10))

    p.add_argument("--ch1-host", default="chs1")
    p.add_argument("--ch2-host", default="chs2")
    p.add_argument("--port", type=int, default=None,
                   help="HTTP(S) port. Defaults to 8123, or 8443 when --secure is set.")
    p.add_argument("--user", default="default")
    p.add_argument("--password", default="")
    p.add_argument("--secure", action="store_true")

    p.add_argument("--connect-timeout", type=int, default=10)
    p.add_argument("--send-receive-timeout", type=int, default=600)

    p.add_argument("--db-pattern", default=DEFAULT_SIGNOZ_DB_PATTERN)

    p.add_argument(
        "--keeper-path-template",
        default="/clickhouse/tables/{shard}/{database}/{table}",
        help="Keeper path template. {database}/{table} are substituted; "
             "{shard} stays literal for ClickHouse macro expansion.",
    )

    p.add_argument("--expected-shard", default="01")
    p.add_argument("--ch1-replica", default="chs1")
    p.add_argument("--ch2-replica", default="chs2")

    p.add_argument("--tmp-suffix", default="__repl_tmp")
    p.add_argument("--old-suffix", default="__old_nonreplicated")

    p.add_argument("--database", default=None, help="Only process one database")
    p.add_argument("--table", default=None, help="Only process one table")

    p.add_argument("--manifest", type=Path, default=Path("./signoz_ch_migration/inventory.json"))
    p.add_argument("--out-dir", type=Path, default=Path("./signoz_ch_migration"))

    p.add_argument("--stop-command", default=None)

    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", action="store_true")

    p.add_argument("--wait-timeout-sec", type=int, default=1800)

    args = p.parse_args()

    if args.execute:
        args.dry_run = False

    if args.port is None:
        args.port = 8443 if args.secure else 8123

    return args


def main():
    args = parse_args()

    if args.phase == 1:
        phase_1_stop_collectors(args)
        return

    ch1, ch2 = connect(args)

    if args.phase == 2:
        phase_2_verify_keeper(ch1, ch2)
    elif args.phase == 3:
        phase_3_verify_macros(
            ch1=ch1,
            ch2=ch2,
            expected_shard=args.expected_shard,
            ch1_replica=args.ch1_replica,
            ch2_replica=args.ch2_replica,
        )
    elif args.phase == 4:
        phase_4_inventory(ch1, args)
    elif args.phase == 5:
        phase_5_create_databases(ch1, ch2, args)
    elif args.phase == 6:
        phase_6_migrate_local_tables_on_chs1(ch1, args)
    elif args.phase == 7:
        phase_7_create_replicated_tables_on_chs2(ch1, ch2, args)
    elif args.phase == 8:
        phase_8_recreate_distributed_tables(ch1, ch2, args)
    elif args.phase == 9:
        phase_9_recreate_views(ch1, ch2, args)
    else:
        raise SystemExit(f"Unsupported phase: {args.phase}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
