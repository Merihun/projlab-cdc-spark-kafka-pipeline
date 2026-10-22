"""Step 3: the same orders at rest in different layouts, and what each costs in files and bytes."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: F401  (date_format makes order_month)

LAYOUTS = ("flat", "by_day", "by_month", "delta_by_month")
CODECS = ("snappy", "gzip", "zstd")


def write_layout(df: DataFrame, path: Path | str, layout: str, codec: str = "snappy") -> None:
    """Write the orders in one layout, compressed with `codec`, replacing what is there:

    flat            Parquet, one file
    by_day          Parquet, one folder per order_date, one file in each
    by_month        Parquet, one folder per order_month (yyyy-MM), one file in each
    delta_by_month  a Delta table partitioned by order_month, one file per month

    Any other layout is a ValueError.
    """
    raise NotImplementedError("step 3: write_layout")


def file_report(path: Path | str) -> dict[str, float]:
    """What a table costs on disk: {"files", "bytes", "partitions", "avg_kb"}. Data files are the *.parquet
    files outside Delta's _delta_log; partitions are the name=value folders that hold them; avg_kb is the
    average file size in KB (1 KB = 1024 bytes), rounded to one decimal (0.0 with no files)."""
    raise NotImplementedError("step 3: file_report")
