"""Step 2: extract the order system's tables with Spark JDBC into the lake's raw zone.

Write each function where it says `raise NotImplementedError`. PROJECT.md, "The lake", has the paths.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: F401  (you will need it for recent_orders)

from fernhill_move import settings  # noqa: F401  (settings.postgres() has the database's settings)
from fernhill_move.tables import TABLES

DRIVER = "org.postgresql.Driver"
FETCH_SIZE = "1000"


def jdbc_url() -> str:
    """jdbc:postgresql://HOST:PORT/DB, from the POSTGRES_* settings."""
    raise NotImplementedError("step 2: jdbc_url")


def jdbc_options(table: str, *, partition_column: str | None = None, lower_bound: int | None = None,
                 upper_bound: int | None = None, num_partitions: int | None = None) -> dict[str, str]:
    """The options of spark.read.format("jdbc") for shop.<table>: url, dbtable, user, password, driver and
    fetchsize. With a partition column, also partitionColumn, lowerBound, upperBound and numPartitions (every
    value a string); a partition column without the three others is a ValueError."""
    raise NotImplementedError("step 2: jdbc_options")


def id_bounds(spark: SparkSession, table: str, column: str) -> tuple[int, int]:
    """The smallest and largest value of a column, asked of PostgreSQL in one small query. Provided."""
    options = jdbc_options(table)
    del options["dbtable"]
    options["query"] = f"SELECT min({column}) AS lo, max({column}) AS hi FROM shop.{table}"
    row = spark.read.format("jdbc").options(**options).load().first()
    return int(row["lo"]), int(row["hi"])


def read_table(spark: SparkSession, table: str, *, partition_column: str | None = None,
               num_partitions: int = 4) -> DataFrame:
    """shop.<table> as a DataFrame; split into num_partitions parallel reads over partition_column, between
    the bounds id_bounds finds."""
    raise NotImplementedError("step 2: read_table")


def recent_orders(spark: SparkSession, since: date) -> DataFrame:
    """Orders placed on or after `since`, read in 4 partitions over order_id. Write the filter in Spark
    (.where); Spark pushes it down into the SQL it sends to PostgreSQL."""
    raise NotImplementedError("step 2: recent_orders")


def raw_path(lake: Path | str, table: str, extracted_on: date) -> Path:
    return Path(lake) / "raw" / table / f"extracted_on={extracted_on.isoformat()}"


def write_raw(df: DataFrame, lake: Path | str, table: str, extracted_on: date) -> Path:
    """Write one day's extract of a table as Parquet at raw_path(...); running it again for the same day
    replaces that day and leaves other days alone. Returns the path."""
    raise NotImplementedError("step 2: write_raw")


PARTITION_COLUMNS = {"customers": "customer_id", "orders": "order_id", "order_items": "order_id",
                     "shipments": "shipment_id"}


def extract(spark: SparkSession, lake: Path | str, extracted_on: date) -> dict[str, int]:
    """Every table of the order system into the raw zone, read in 4 partitions. Returns rows per table.
    Provided: it uses your read_table and write_raw."""
    counts = {}
    for table in TABLES:
        df = read_table(spark, table, partition_column=PARTITION_COLUMNS[table], num_partitions=4)
        write_raw(df, lake, table, extracted_on)
        counts[table] = spark.read.parquet(str(raw_path(lake, table, extracted_on))).count()
    return counts
