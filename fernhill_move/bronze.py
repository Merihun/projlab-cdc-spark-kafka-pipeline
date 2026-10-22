"""Step 5: every Debezium record from Kafka into the bronze Delta table, exactly once, with Spark Structured
Streaming."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: F401
from pyspark.sql.streaming import StreamingQuery


def kafka_options(bootstrap: str) -> dict[str, str]:
    """The options of spark.readStream.format("kafka"): kafka.bootstrap.servers, a subscribePattern that
    matches the four fernhill.shop.* topics (and nothing else), and startingOffsets earliest."""
    raise NotImplementedError("step 5: kafka_options")


def read_kafka(spark: SparkSession, bootstrap: str) -> DataFrame:
    """Provided: the Kafka stream with your options."""
    return spark.readStream.format("kafka").options(**kafka_options(bootstrap)).load()


def bronze_frame(kafka_df: DataFrame) -> DataFrame:
    """Kafka's columns as the bronze table keeps them (PROJECT.md, "Bronze"): topic, partition, offset,
    kafka_ts, key, value, table_name, op, lsn, source_ts_ms, in that order."""
    raise NotImplementedError("step 5: bronze_frame")


def start_bronze(stream_df: DataFrame, bronze_path: Path | str, checkpoint: Path | str) -> StreamingQuery:
    """Start the query that appends bronze_frame(stream_df) to the Delta table at bronze_path, with its
    checkpoint at `checkpoint`, and stops by itself once it has caught up (trigger availableNow). Returns the
    started query."""
    raise NotImplementedError("step 5: start_bronze")


def run_bronze(spark: SparkSession, bootstrap: str, bronze_path: Path | str, checkpoint: Path | str) -> dict | None:
    """Provided: read Kafka into bronze until caught up; returns the last progress report of the run."""
    query = start_bronze(read_kafka(spark, bootstrap), bronze_path, checkpoint)
    query.awaitTermination()
    return query.lastProgress
