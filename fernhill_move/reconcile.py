"""Step 8: prove the stream right. Silver orders (built from the change stream) and a fresh batch snapshot of
shop.orders are summarised per order_date and compared day by day."""

from __future__ import annotations

import zlib  # noqa: F401
from collections.abc import Iterable
from datetime import date
from decimal import Decimal
from typing import Any, NamedTuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: F401

MODULUS = 2**32


class Day(NamedTuple):
    orders: int
    amount: Decimal
    checksum: int


def row_checksum(order: dict[str, Any]) -> int:
    """CRC-32 of "order_id|customer_id|status|total_amount", e.g. "1001|59|delivered|47.97"."""
    raise NotImplementedError("step 8: row_checksum")


def daily_summary(orders: Iterable[dict[str, Any]]) -> dict[date, Day]:
    """Per order_date, oldest first: Day(orders, sum of total_amount, sum of row_checksum modulo 2**32)."""
    raise NotImplementedError("step 8: daily_summary")


def daily_summary_df(orders: DataFrame) -> DataFrame:
    """daily_summary in Spark: columns order_date, orders, amount, checksum, one row per day."""
    raise NotImplementedError("step 8: daily_summary_df")


def collect_summary(summary: DataFrame) -> dict[date, Day]:
    """Provided: daily_summary_df's rows as daily_summary's dict."""
    return {r["order_date"]: Day(r["orders"], r["amount"], r["checksum"]) for r in summary.collect()}


def compare(stream: dict[date, Day], batch: dict[date, Day]) -> list[dict[str, Any]]:
    """The days that differ, oldest first: {"day": "YYYY-MM-DD", "kind": ..., "stream": [orders, amount as
    text, checksum] or None, "batch": the same}. Kinds, in this order of precedence: missing_in_stream,
    missing_in_batch, count, amount, content."""
    raise NotImplementedError("step 8: compare")


def reconcile(silver_orders: DataFrame, batch_orders: DataFrame) -> dict[str, Any]:
    """Compare silver orders, without deleted rows, with a batch snapshot of shop.orders:
    {"days_compared", "stream": {"orders", "amount"}, "batch": {...}, "differences": compare(...)}."""
    raise NotImplementedError("step 8: reconcile")
