"""Re-sends records that are already in Kafka, exactly as they are: what a consumer sees when a connector
restarts after a crash and delivers again everything since the last offsets it saved (at-least-once)."""

from __future__ import annotations

import os

TOPICS = ("fernhill.shop.orders", "fernhill.shop.shipments")


def redeliver(last: int = 40, topics: tuple[str, ...] = TOPICS, bootstrap: str | None = None) -> dict[str, int]:
    """Produce the last `last` records of every partition of `topics` again, to the same partition, with the
    same key, value and headers. Returns how many records were re-sent per topic."""
    from confluent_kafka import Consumer, Producer, TopicPartition

    servers = bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = Consumer({"bootstrap.servers": servers, "group.id": "fernhill-redeliver", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": servers})
    sent: dict[str, int] = {}
    try:
        for topic in topics:
            meta = consumer.list_topics(topic, timeout=10).topics[topic]
            if meta.error is not None or not meta.partitions:
                raise SystemExit(f"Topic {topic} not found: is the connector registered and has traffic run?")
            sent[topic] = 0
            for partition in sorted(meta.partitions):
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
                start = max(low, high - last)
                if start >= high:
                    continue
                consumer.assign([TopicPartition(topic, partition, start)])
                position = start
                while position < high:
                    msg = consumer.poll(10)
                    if msg is None:
                        raise SystemExit(f"Timed out reading {topic} partition {partition} at offset {position}")
                    if msg.error():
                        raise SystemExit(str(msg.error()))
                    producer.produce(topic, key=msg.key(), value=msg.value(), partition=partition, headers=msg.headers())
                    position = msg.offset() + 1
                    sent[topic] += 1
        producer.flush(30)
    finally:
        consumer.close()
    return sent
