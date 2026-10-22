"""Steps 6 and 7: from bronze to silver. Each micro-batch's changes are parsed per table, reduced to the
latest change per key, and merged into that table's Delta table, so silver shows each row as PostgreSQL has
it, deleted rows included (marked _deleted)."""

from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable  # noqa: F401
from pyspark.sql import Column, DataFrame, SparkSession, Window  # noqa: F401
from pyspark.sql import functions as F  # noqa: F401

from fernhill_move.tables import COLUMNS, KEYS, TABLES, WIRE  # noqa: F401  (KEYS and TABLES: yours to use)


def wire_schema(table: str) -> str:
    """Provided: the Debezium event of a table as a Spark DDL schema, each column as it travels in the JSON."""
    row = ", ".join(f"{name}: {WIRE[kind]}" for name, kind in COLUMNS[table])
    return f"before STRUCT<{row}>, after STRUCT<{row}>, op STRING, source STRUCT<lsn: BIGINT, ts_ms: BIGINT>"


def to_lake_type(value: Column, kind: str) -> Column:
    """A column as Debezium wrote it, turned into its lake type (tables.py): DATE, DECIMAL(10,2) or
    TIMESTAMP; INT and STRING as they are."""
    raise NotImplementedError("step 6: to_lake_type")


def parse_table(bronze: DataFrame, table: str) -> DataFrame:
    """One table's change events from bronze rows: the table's columns (from `after`, or from `before` for a
    delete), typed as in tables.py, then _deleted (op is d), _lsn and _source_ts_ms. Tombstones are dropped."""
    raise NotImplementedError("step 6: parse_table")


def latest_changes(changes: DataFrame, table: str) -> DataFrame:
    """One row per key (KEYS[table]): the change with the highest _lsn."""
    raise NotImplementedError("step 6: latest_changes")


def merge_batch(spark: SparkSession, changes: DataFrame, path: Path | str, table: str) -> None:
    """Apply one batch of latest changes to the table's silver Delta table at `path` (the first batch creates
    it). A row is replaced only by a change with a higher _lsn; a key silver doesn't have is inserted."""
    raise NotImplementedError("step 6: merge_batch")


def apply_bronze_batch(spark: SparkSession, batch: DataFrame, silver_root: Path | str) -> None:
    """Every table's changes in one bronze micro-batch, merged into silver_root/<table>."""
    raise NotImplementedError("step 6: apply_bronze_batch")


def run_silver(spark: SparkSession, bronze_path: Path | str, silver_root: Path | str, checkpoint: Path | str) -> dict | None:
    """Stream bronze's new rows (a Delta streaming source) into silver with foreachBatch and
    apply_bronze_batch, checkpointed at `checkpoint`, until caught up (availableNow); wait for it to finish
    and return its last progress report."""
    raise NotImplementedError("step 6: run_silver")
