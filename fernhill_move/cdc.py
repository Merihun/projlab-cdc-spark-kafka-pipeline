"""Steps 4 and 6: Debezium change events in plain Python, without Spark or Kafka.

The same rules the Spark jobs follow, small enough to test with a handful of events: what an event says
(parse_change), what it does to a row (action), which event wins for a key (latest_per_key), and what a table
holds once changes are applied (apply_changes).
"""

from __future__ import annotations

import json  # noqa: F401
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta  # noqa: F401
from decimal import Decimal  # noqa: F401
from typing import Any

from fernhill_move.tables import COLUMNS, KEYS  # noqa: F401

EPOCH = date(1970, 1, 1)


@dataclass(frozen=True)
class Change:
    """One row change. `row` is the row after it (for a delete: the row before it, the last thing known
    about the deleted row); `lsn` is the change's position in PostgreSQL's log (source.lsn), and
    `source_ts_ms` when it was committed (source.ts_ms)."""

    table: str
    op: str
    key: tuple
    row: dict[str, Any]
    lsn: int
    source_ts_ms: int


def action(op: str) -> str:
    """What a Debezium op does to the row: "upsert" for r, c and u, "delete" for d. Anything else is a
    ValueError."""
    raise NotImplementedError("step 4: action")


def decode(table: str, row: dict[str, Any]) -> dict[str, Any]:
    """A row as Debezium wrote it, with each value as its lake type (tables.py): a DATE from days since
    1970-01-01, a DECIMAL from its string, a TIMESTAMP from its ISO string. None stays None; columns
    tables.py does not know are kept as they came."""
    raise NotImplementedError("step 4: decode")


def parse_change(key: str | bytes | None, value: str | bytes | None) -> Change | None:
    """One Kafka record from Debezium as a Change; None for a tombstone (value None). The table is
    source.table, the key the values of KEYS[table] in the record's key, the row decoded."""
    raise NotImplementedError("step 4: parse_change")


def latest_per_key(changes: Iterable[Change]) -> list[Change]:
    """For each (table, key), the change with the highest lsn. Sorted by table, then key."""
    raise NotImplementedError("step 6: latest_per_key")


def apply_changes(state: dict[tuple, Change], changes: Iterable[Change]) -> dict[tuple, Change]:
    """A silver table in plain Python: {(table, key): the change that made the row as it is}. A change
    replaces what is held only when its lsn is higher; a delete is kept as the change with op d. Returns a
    new dict; `state` is not changed."""
    raise NotImplementedError("step 6: apply_changes")
