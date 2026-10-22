"""Step 9: operate the pipeline. How far behind Kafka the bronze stream is (lag, from its checkpoint, since a
Spark query keeps its offsets there rather than in a consumer group), and replaying Kafka from a moment in
time into silver."""

from __future__ import annotations

import json  # noqa: F401
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from fernhill_move.bronze import bronze_frame


def checkpoint_offsets(checkpoint: Path | str) -> dict[str, dict[int, int]]:
    """{topic: {partition: next offset to read}} from the newest batch that has a file in both
    <checkpoint>/offsets and <checkpoint>/commits (the offsets file's third line). {} when there is none."""
    raise NotImplementedError("step 9: checkpoint_offsets")


def end_offsets(bootstrap: str, topics: list[str]) -> dict[str, dict[int, int]]:
    """Provided: the offset the next record of each partition will get (Kafka's high watermark)."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "fernhill-lag", "enable.auto.commit": False})
    try:
        out: dict[str, dict[int, int]] = {}
        for topic in topics:
            meta = consumer.list_topics(topic, timeout=10).topics[topic]
            out[topic] = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                          for p in sorted(meta.partitions)}
        return out
    finally:
        consumer.close()


def lag(end: dict[str, dict[int, int]], processed: dict[str, dict[int, int]]) -> dict[str, Any]:
    """{"topics": {topic: {"partitions": {"0": {"end", "processed", "lag"}, ...}, "lag": sum}}, "total": sum}.
    A partition the query has never read counts from 0."""
    raise NotImplementedError("step 9: lag")


def replay_options(bootstrap: str, topics: list[str], since: datetime, partitions: int = 3) -> dict[str, str]:
    """The options of spark.read.format("kafka") that read every record written at or after `since`, to the
    end: kafka.bootstrap.servers, subscribePattern, startingOffsetsByTimestamp (JSON),
    startingOffsetsByTimestampStrategy latest, endingOffsets latest."""
    raise NotImplementedError("step 9: replay_options")


def replay(spark: SparkSession, bootstrap: str, topics: list[str], since: datetime) -> DataFrame:
    """Provided: Kafka's records since `since`, as bronze rows (a batch read: it stops at the end)."""
    return bronze_frame(spark.read.format("kafka").options(**replay_options(bootstrap, topics, since)).load())


def merge_metrics(spark: SparkSession, path: Path | str, since_version: int) -> dict[str, int]:
    """{"inserted", "updated", "deleted"}: the rows the MERGEs on the Delta table at `path` after version
    `since_version` inserted, updated and deleted, summed from DESCRIBE HISTORY's operationMetrics."""
    raise NotImplementedError("step 9: merge_metrics")
