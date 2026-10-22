"""Writes the shop's rows and traffic into the lab's PostgreSQL (psycopg). The rows come from data.py."""

from __future__ import annotations

import os
import time
from typing import Any

from fernhill_shop import data

SLOT = "fernhill_slot"


def conninfo() -> dict[str, Any]:
    """The lab database, from the POSTGRES_* variables compose.yaml sets in the workspace."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "user": os.environ.get("POSTGRES_USER", "projlab"),
        "password": os.environ.get("POSTGRES_PASSWORD", "projlab"),
        "dbname": os.environ.get("POSTGRES_DB", "projlab"),
        "connect_timeout": 5,
    }


def connect():
    import psycopg  # only the commands that talk to the database need it

    # autocommit: each `with conn.transaction()` is then a real transaction, committed when the block ends.
    return psycopg.connect(**conninfo(), autocommit=True)


def _ran(conn) -> dict[str, int]:
    return dict(conn.execute("SELECT phase, changes FROM shop.simulator_log").fetchall())


def seed(conn) -> dict[str, int]:
    """Load the rows of 1 October 2026, 08:00 into the empty tables. Refuses to load twice."""
    if "seed" in _ran(conn):
        raise SystemExit("The shop is already seeded. To start the day again from nothing: make reset-day YES=1, then make seed.")
    counts = {}
    with conn.transaction():
        for table in data.TABLES:
            columns = data.COLUMNS[table]
            rows = data.seed_rows()[table]
            with conn.cursor().copy(f"COPY {data.SCHEMA}.{table} ({', '.join(columns)}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(tuple(row[c] for c in columns))
            counts[table] = len(rows)
        conn.execute("INSERT INTO shop.simulator_log (phase, changes) VALUES ('seed', %s)", (sum(counts.values()),))
    return counts


def _sql(op: data.Op) -> tuple[str, tuple]:
    table = f"{data.SCHEMA}.{op.table}"
    if op.kind == "insert":
        columns = list(op.row)
        return (f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
                tuple(op.row[c] for c in columns))
    where = " AND ".join(f"{k} = %s" for k in op.key)
    if op.kind == "update":
        sets = ", ".join(f"{c} = %s" for c in op.row)
        return f"UPDATE {table} SET {sets} WHERE {where}", (*op.row.values(), *op.key.values())
    if op.kind == "delete":
        return f"DELETE FROM {table} WHERE {where}", tuple(op.key.values())
    if op.kind == "add_column":
        (column, sql_type), = op.row.items()
        return f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}", ()
    raise ValueError(f"unknown change {op.kind!r}")


def slot_exists(conn) -> bool:
    return conn.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone() is not None


def run_phase(conn, phase: str, pause: float = 0.05) -> int:
    """Commit the phase's transactions one by one, in order. Each phase runs once, after the one before it,
    and only once the connector's replication slot exists: otherwise its changes would never be captured."""
    if phase not in data.PHASES:
        raise SystemExit(f"Unknown phase {phase!r}: one of {', '.join(data.PHASES)}")
    ran = _ran(conn)
    if "seed" not in ran:
        raise SystemExit("Seed the shop first: python -m fernhill_shop seed")
    if phase in ran:
        raise SystemExit(f"The {phase} traffic has already run. Every phase runs once; see python -m fernhill_shop status.")
    before = data.PHASES[data.PHASES.index(phase) - 1] if phase != data.PHASES[0] else None
    if before and before not in ran:
        raise SystemExit(f"Run the {before} traffic first: python -m fernhill_shop traffic --phase {before}")
    if not slot_exists(conn):
        raise SystemExit(f"No replication slot {SLOT!r} yet: register the Debezium connector first (step 4), "
                         "or these changes would never reach Kafka.")
    changes = 0
    for tx in data.plan()[phase]:
        with conn.transaction():
            for op in tx.ops:
                conn.execute(*_sql(op))
                changes += op.kind != "add_column"
        if pause and phase != "burst":
            time.sleep(pause)
    conn.execute("INSERT INTO shop.simulator_log (phase, changes) VALUES (%s, %s)", (phase, changes))
    return changes


def status(conn) -> dict[str, Any]:
    counts = {t: conn.execute(f"SELECT count(*) FROM {data.SCHEMA}.{t}").fetchone()[0] for t in data.TABLES}
    slot = conn.execute("SELECT active, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s",
                        (SLOT,)).fetchone()
    return {
        "rows": counts,
        "phases": [p for p in ("seed", *data.PHASES) if p in _ran(conn)],
        "slot": {"name": SLOT, "active": slot[0], "confirmed_flush_lsn": slot[1]} if slot else None,
    }


def statements(text: str) -> list[str]:
    """The SQL statements of a file: comment lines dropped, split on the ; that ends a line."""
    lines = [line for line in text.splitlines() if not line.strip().startswith("--")]
    out, current = [], []
    for line in lines:
        current.append(line)
        if line.rstrip().endswith(";"):
            out.append("\n".join(current).strip().rstrip(";"))
            current = []
    if "".join(current).strip():
        out.append("\n".join(current).strip().rstrip(";"))
    return [s for s in out if s.strip()]


def table_text(columns: list[str], rows: list[tuple]) -> str:
    """Rows as psql prints them: a header, a rule, the rows, and (n rows)."""
    cells = [["" if v is None else str(v) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    head = " " + " | ".join(c.center(w) for c, w in zip(columns, widths, strict=True))
    rule = "-" + "-+-".join("-" * w for w in widths) + "-"
    body = [" " + " | ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)) for r in cells]
    return "\n".join([head, rule, *body, f"({len(rows)} row{'' if len(rows) == 1 else 's'})"])


def run_sql(conn, text: str) -> list[str]:
    """Run each statement; a query's rows come back as a table, any other statement as its status."""
    out = []
    for statement in statements(text):
        cur = conn.execute(statement)
        if cur.description is None:
            out.append(cur.statusmessage or "")
        else:
            out.append(table_text([d.name for d in cur.description], cur.fetchall()))
    return out


def describe(conn, table: str) -> str:
    """A table's columns, its primary key and its replica identity, as \\d shows them."""
    schema, _, name = table.rpartition(".")
    schema = schema or data.SCHEMA
    columns = conn.execute(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod), CASE WHEN a.attnotnull THEN 'not null' ELSE '' END "
        "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
        (schema, name)).fetchall()
    if not columns:
        raise SystemExit(f"No table {schema}.{name}")
    key = conn.execute(
        "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey) "
        "WHERE i.indrelid = %s::regclass AND i.indisprimary ORDER BY array_position(i.indkey::int2[], a.attnum)",
        (f"{schema}.{name}",)).fetchall()
    identity = conn.execute("SELECT relreplident FROM pg_class WHERE oid = %s::regclass", (f"{schema}.{name}",)).fetchone()[0]
    return "\n".join([f'Table "{schema}.{name}"', table_text(["Column", "Type", "Nullable"], columns),
                      f"Primary key: ({', '.join(k for (k,) in key)})", f"Replica identity: {identity}"])


def reset(conn) -> None:
    """Empty the shop as it was before make seed: no rows, no new column, no phases, no slot, no publication.
    The connector must be gone first (its slot is in use while it runs)."""
    for _ in range(30):
        slot = conn.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone()
        if slot is None:
            break
        if not slot[0]:
            conn.execute("SELECT pg_drop_replication_slot(%s)", (SLOT,))
            break
        time.sleep(1)
    else:
        raise SystemExit(f"The replication slot {SLOT} is still in use: is the connector still running?")
    conn.execute("DROP PUBLICATION IF EXISTS fernhill_publication")
    with conn.transaction():
        conn.execute(f"TRUNCATE {', '.join(f'{data.SCHEMA}.{t}' for t in data.TABLES)}, shop.simulator_log")
        conn.execute(f"ALTER TABLE {data.SCHEMA}.{data.NEW_COLUMN[0]} DROP COLUMN IF EXISTS {data.NEW_COLUMN[1]}")
