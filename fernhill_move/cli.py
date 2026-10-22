"""python -m fernhill_move COMMAND: the project's commands, one per thing a step runs. Provided: each command
calls the functions you write, so it works once the step that writes them is done. `make` lists them."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

from fernhill_move import settings
from fernhill_move.tables import TABLES, topic

FORMATS = settings.LAKE / "formats"


def _write_json(path: str, data: object) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"wrote {out}")


def _spark():
    from fernhill_move.spark import get_spark

    return get_spark()


def _rest(method: str, path: str) -> tuple[int, object]:
    """One call to Kafka Connect's REST API: (status, JSON). Status 0 when Connect does not answer."""
    import urllib.error
    import urllib.request

    request = urllib.request.Request(f"{settings.CONNECT_URL.rstrip('/')}{path}", method=method,
                                     headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - the lab's own Connect
            body = response.read()
            return response.status, json.loads(body) if body else None
    except urllib.error.HTTPError as error:
        body = error.read()
        return error.code, json.loads(body) if body else None
    except (urllib.error.URLError, OSError) as error:
        return 0, str(error)


def _ends(names: list[str]) -> dict[str, dict[int, int]]:
    """End offsets of the topics that exist ({} for the others), without creating any."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS, "group.id": "fernhill-tools",
                         "enable.auto.commit": False, "allow.auto.create.topics": False})
    try:
        out = {}
        listed = consumer.list_topics(timeout=10).topics
        for name in names:
            meta = listed.get(name)
            if meta is None or meta.error is not None:
                continue
            out[name] = {p: consumer.get_watermark_offsets(TopicPartition(name, p), timeout=10)[1] for p in sorted(meta.partitions)}
        return out
    finally:
        consumer.close()


def _settle(quiet: float = 6.0, timeout: float = 180.0) -> dict[str, dict[int, int]]:
    """Wait until Debezium has written what it has read: the four topics' end offsets unchanged for `quiet`
    seconds. A command that reads Kafka right after traffic would otherwise miss the last changes."""
    names = [topic(t) for t in TABLES]
    print(f"waiting for Debezium to catch up (end offsets unchanged for {quiet:.0f} s) ...", flush=True)
    start = steady = time.monotonic()
    last = _ends(names)
    while time.monotonic() - steady < quiet:
        if time.monotonic() - start > timeout:
            print("  still moving after 3 minutes: is the connector RUNNING? (make connector-status)")
            break
        time.sleep(1)
        now = _ends(names)
        if now != last:
            last, steady = now, time.monotonic()
    status, body = _rest("GET", "/connectors/fernhill-shop/status")
    if status == 200 and isinstance(body, dict) and body.get("connector", {}).get("state") != "RUNNING":
        print(f"  the connector is {body.get('connector', {}).get('state')}: make connector-status")
    print(f"  caught up: {sum(o for parts in last.values() for o in parts.values())} records in the four topics")
    return last


def cmd_extract(args) -> None:
    from fernhill_move.batch import extract

    day = date.fromisoformat(args.day) if args.day else date.today()
    for table, rows in extract(_spark(), settings.LAKE, day).items():
        print(f"{table:<12} {rows:>6} rows -> {settings.LAKE}/raw/{table}/extracted_on={day}")


def cmd_recent(args) -> None:
    from fernhill_move.batch import recent_orders

    df = recent_orders(_spark(), date.fromisoformat(args.since))
    print(f"orders since {args.since}: {df.count()} rows in {df.rdd.getNumPartitions()} partitions")
    df.explain()


def _latest_raw(table: str) -> Path:
    days = sorted((settings.LAKE / "raw" / table).glob("extracted_on=*"))
    if not days:
        sys.exit(f"No extract of {table} in {settings.LAKE}/raw: run make extract first (step 2).")
    return days[-1]


def cmd_formats(args) -> None:
    from fernhill_move.formats import CODECS, file_report, write_layout

    import shutil

    spark = _spark()
    orders = spark.read.parquet(str(_latest_raw("orders")))
    shutil.rmtree(FORMATS, ignore_errors=True)  # every run starts from nothing, so the report is the same each time
    targets = [(f"flat_{codec}", "flat", codec) for codec in CODECS]
    targets += [("by_day", "by_day", "snappy"), ("by_month", "by_month", "snappy"), ("delta_by_month", "delta_by_month", "snappy")]
    for name, layout, codec in targets:
        write_layout(orders, FORMATS / name, layout, codec)
    # The Delta table is written a second time, as tomorrow's run would: what happens to its files?
    write_layout(orders, FORMATS / "delta_by_month", "delta_by_month")
    print(f"{'layout':<16}{'files':>7}{'partitions':>12}{'bytes':>10}{'avg KB':>9}")
    for name, _, _ in targets:
        r = file_report(FORMATS / name)
        print(f"{name:<16}{r['files']:>7}{r['partitions']:>12}{r['bytes']:>10}{r['avg_kb']:>9}")
    from delta.tables import DeltaTable

    table = DeltaTable.forPath(spark, str(FORMATS / "delta_by_month"))
    print(f"delta_by_month: version {table.history(1).first()['version']} is current and reads {table.detail().first()['numFiles']} files")


def cmd_register(args) -> None:
    from fernhill_move.connector import connector_status, load_config, register

    document = load_config(args.file)
    register(document, settings.CONNECT_URL)
    print(f"registered {document['name']} at {settings.CONNECT_URL}")
    status: dict = {}
    for _ in range(30):  # a new connector has no status for a moment, until its task starts
        try:
            status = connector_status(document["name"], settings.CONNECT_URL)
        except RuntimeError:
            time.sleep(1)
            continue
        if status.get("tasks"):
            break
        time.sleep(1)
    print(json.dumps(status, indent=2))


def cmd_status(args) -> None:
    import urllib.error

    from fernhill_move.connector import connector_status

    try:
        print(json.dumps(connector_status(args.name, settings.CONNECT_URL), indent=2))
    except (urllib.error.URLError, ConnectionError) as error:
        sys.exit(f"Kafka Connect does not answer at {settings.CONNECT_URL} ({getattr(error, 'reason', error)}): "
                 "it is starting or restarting; try again in a few seconds.")


def cmd_connector_restart(args) -> None:
    """Restart the connector and its task through the REST API: they stop (saving their offsets) and start
    again from those offsets. The Connect worker itself keeps running."""
    status, body = _rest("POST", f"/connectors/{args.name}/restart?includeTasks=true")
    if status not in (200, 202, 204):
        sys.exit(f"Connect answered {status}: {body}")
    print(f"restart of {args.name} requested ({status})")
    for _ in range(30):
        time.sleep(1)
        status, body = _rest("GET", f"/connectors/{args.name}/status")
        states = [body["connector"]["state"], *(t["state"] for t in body["tasks"])] if status == 200 and isinstance(body, dict) else []
        if states and all(state == "RUNNING" for state in states):
            break
    print(json.dumps(body, indent=2))


def cmd_topics(args) -> None:
    """Every topic in Kafka with its partitions' end offsets (like kafka-topics.sh --list and kafka-get-offsets.sh)."""
    from confluent_kafka import Consumer

    consumer = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS, "group.id": "fernhill-tools",
                         "allow.auto.create.topics": False})
    try:
        names = sorted(consumer.list_topics(timeout=10).topics)
    finally:
        consumer.close()
    ends = _ends([n for n in names if n.startswith("fernhill.")])
    for name in names:
        if name in ends:
            print(f"{name:<28} " + "  ".join(f"{p}:{o}" for p, o in ends[name].items()))
        else:
            print(name)


def cmd_peek(args) -> None:
    """Records of one topic, from an offset (like kafka-console-consumer.sh with print.key and print.offset)."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS, "group.id": "fernhill-tools",
                         "enable.auto.commit": False, "allow.auto.create.topics": False})
    try:
        meta = consumer.list_topics(args.topic, timeout=10).topics.get(args.topic)
        if meta is None or meta.error is not None:
            sys.exit(f"No topic {args.topic}: make topics lists them")
        consumer.assign([TopicPartition(args.topic, args.partition, args.offset)])
        _, high = consumer.get_watermark_offsets(TopicPartition(args.topic, args.partition), timeout=10)
        shown = 0
        while shown < args.n and args.offset + shown < high:
            msg = consumer.poll(10)
            if msg is None or msg.error():
                break
            key = msg.key().decode() if msg.key() is not None else None
            value = msg.value().decode() if msg.value() is not None else None
            print(f"Partition:{msg.partition()}\tOffset:{msg.offset()}\t{key}\t{value}")
            shown += 1
    finally:
        consumer.close()


def cmd_groups(args) -> None:
    """Kafka's consumer groups and what each has committed (like kafka-consumer-groups.sh --list)."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    groups = sorted(g.group_id for g in admin.list_consumer_groups().result().valid)
    print("\n".join(groups) if groups else "(no consumer groups: nothing has committed offsets to Kafka)")


def cmd_reset_day(args) -> None:
    """Back to before make seed: the connector and its saved offsets, the shop's rows, the slot, the topics,
    and the lake's bronze, silver and gold with their checkpoints. Then: make seed, make register, and the
    traffic again, phase by phase."""
    import shutil

    from confluent_kafka.admin import AdminClient

    from fernhill_shop import db

    if not args.yes:
        sys.exit("This empties the shop, Kafka's fernhill topics and the lake's bronze, silver and gold: make reset-day YES=1")
    status, _ = _rest("GET", f"/connectors/{args.name}")
    if status == 200:
        _rest("PUT", f"/connectors/{args.name}/stop")
        for _ in range(30):
            time.sleep(1)
            status, body = _rest("GET", f"/connectors/{args.name}/status")
            if status == 200 and body["connector"]["state"] == "STOPPED":
                break
        status, body = _rest("DELETE", f"/connectors/{args.name}/offsets")
        print(f"connector {args.name}: offsets reset ({status})")
        _rest("DELETE", f"/connectors/{args.name}")
        print(f"connector {args.name}: deleted")
    with db.connect() as conn:
        db.reset(conn)
    print("shop: emptied, slot and publication dropped")
    names = [topic(t) for t in TABLES]
    existing = list(_ends(names))
    if existing:
        admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
        for future in admin.delete_topics(existing).values():
            future.result()
        for _ in range(30):
            if not _ends(names):
                break
            time.sleep(1)
        print(f"kafka: deleted {', '.join(existing)}")
    for path in (settings.BRONZE.parent, settings.SILVER, settings.GOLD, settings.CHECKPOINTS):
        shutil.rmtree(path, ignore_errors=True)
    print(f"lake: removed bronze, silver, gold and {settings.CHECKPOINTS} (raw and formats are kept)")
    print("Next: make seed, make register, then the traffic again phase by phase.")


def cmd_capture(args) -> None:
    """Every record of the four topics, from the start to now; keeps all but the snapshot reads (op r), of
    which it keeps the first of each topic, so the file stays small."""
    from confluent_kafka import Consumer, TopicPartition

    _settle()
    consumer = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS, "group.id": "fernhill-capture",
                         "enable.auto.commit": False})
    kept, read = [], 0
    try:
        for name in (topic(t) for t in TABLES):
            meta = consumer.list_topics(name, timeout=10).topics[name]
            if meta.error is not None or not meta.partitions:
                sys.exit(f"Topic {name} not found: is the connector registered (make register)?")
            first_read = False
            for partition in sorted(meta.partitions):
                low, high = consumer.get_watermark_offsets(TopicPartition(name, partition), timeout=10)
                consumer.assign([TopicPartition(name, partition, low)])
                position = low
                while position < high:
                    msg = consumer.poll(10)
                    if msg is None or msg.error():
                        sys.exit(f"Could not read {name} partition {partition} at offset {position}: {msg and msg.error()}")
                    position = msg.offset() + 1
                    read += 1
                    value = msg.value().decode() if msg.value() is not None else None
                    is_read = value is not None and json.loads(value).get("op") == "r"
                    if is_read and first_read:
                        continue
                    first_read = first_read or is_read
                    kept.append({"topic": name, "partition": partition, "offset": msg.offset(),
                                 "key": msg.key().decode() if msg.key() is not None else None, "value": value})
    finally:
        consumer.close()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(json.dumps(r) + "\n" for r in kept), encoding="utf-8")
    print(f"read {read} records, wrote {len(kept)} to {out}")


def cmd_bronze(args) -> None:
    from fernhill_move.bronze import run_bronze

    _settle()
    spark = _spark()
    progress = run_bronze(spark, settings.KAFKA_BOOTSTRAP_SERVERS, settings.BRONZE, settings.CHECKPOINTS / "bronze")
    rows = spark.read.format("delta").load(str(settings.BRONZE)).count()
    print(f"bronze: {rows} rows in {settings.BRONZE}")
    if progress is not None:
        _write_json(args.out, progress)


def cmd_silver(args) -> None:
    from fernhill_move.silver import run_silver

    spark = _spark()
    run_silver(spark, settings.BRONZE, settings.SILVER, settings.CHECKPOINTS / "silver")
    for table in TABLES:
        path = settings.SILVER / table
        if path.is_dir():
            df = spark.read.format("delta").load(str(path))
            print(f"silver.{table:<12} {df.count():>6} rows, {df.where('_deleted').count()} deleted")


def cmd_show(args) -> None:
    from pyspark.sql import functions as F

    from fernhill_move.tables import KEYS

    spark = _spark()
    df = spark.read.format("delta").load(str(settings.SILVER / args.table))
    for column, value in zip(KEYS[args.table], args.key, strict=True):
        df = df.where(F.col(column) == int(value))
    df.show(truncate=False, vertical=True)


def cmd_contract(args) -> None:
    """The columns Debezium sends for each table (the keys of `after` in bronze), against tables.py."""
    from pyspark.sql import functions as F

    from fernhill_move.tables import COLUMNS

    spark = _spark()
    bronze = spark.read.format("delta").load(str(settings.BRONZE)).where(F.col("value").isNotNull())
    keys = bronze.select("table_name", F.explode(F.expr("json_object_keys(get_json_object(value, '$.after'))")).alias("column"))
    seen = {}
    for row in keys.distinct().collect():
        seen.setdefault(row["table_name"], set()).add(row["column"])
    for table in TABLES:
        known = [name for name, _ in COLUMNS[table]]
        new = sorted(seen.get(table, set()) - set(known))
        print(f"{table:<12} tables.py: {len(known)} columns; in bronze but not in tables.py: {', '.join(new) if new else 'none'}")


def cmd_reconcile(args) -> None:
    from fernhill_move.batch import read_table
    from fernhill_move.reconcile import reconcile

    spark = _spark()
    reader = spark.read.format("delta")
    if args.version is not None:  # time travel: silver orders as they were at that version
        reader = reader.option("versionAsOf", args.version)
    silver = reader.load(str(settings.SILVER / "orders"))
    result = reconcile(silver, read_table(spark, "orders", partition_column="order_id"))
    result = {"compared_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **result}
    print(f"days compared: {result['days_compared']}; stream {result['stream']}; batch {result['batch']}")
    for d in result["differences"]:
        print(f"  {d['day']}  {d['kind']:<18} stream={d['stream']}  batch={d['batch']}")
    print(f"{len(result['differences'])} days differ")
    _write_json(args.out, result)


def cmd_lag(args) -> None:
    from fernhill_move.operate import checkpoint_offsets, end_offsets, lag

    _settle()
    topics = [topic(t) for t in TABLES]
    result = lag(end_offsets(settings.KAFKA_BOOTSTRAP_SERVERS, topics), checkpoint_offsets(settings.CHECKPOINTS / "bronze"))
    result = {"measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **result}
    for name, t in result["topics"].items():
        print(f"{name:<28} lag {t['lag']:>6}   " + "  ".join(f"p{p}: {r['processed']}/{r['end']}" for p, r in t["partitions"].items()))
    print(f"total lag: {result['total']} records")
    _write_json(args.out, result)


def cmd_backfill(args) -> None:
    from fernhill_move.operate import merge_metrics, replay
    from fernhill_move.silver import apply_bronze_batch

    spark = _spark()
    since = datetime.fromisoformat(args.since)
    if since.tzinfo is None:
        sys.exit("--since needs a time zone, e.g. 2026-10-20T09:30:00+00:00")
    records = replay(spark, settings.KAFKA_BOOTSTRAP_SERVERS, [topic(t) for t in TABLES], since)
    counts = {r["topic"]: r["count"] for r in records.groupBy("topic").count().collect()}
    versions = {}
    from delta.tables import DeltaTable

    for table in TABLES:
        versions[table] = DeltaTable.forPath(spark, str(settings.SILVER / table)).history(1).first()["version"]
    apply_bronze_batch(spark, records, settings.SILVER)
    merged = {table: merge_metrics(spark, settings.SILVER / table, versions[table]) for table in TABLES}
    result = {"since": since.isoformat(), "records": dict(sorted(counts.items())), "silver": merged}
    print(json.dumps(result, indent=2))
    _write_json(args.out, result)


def cmd_serve(args) -> None:
    from fernhill_move.serving import orders_in_flight

    spark = _spark()
    as_of = date.fromisoformat(args.as_of) if args.as_of else date.today()
    orders = spark.read.format("delta").load(str(settings.SILVER / "orders"))
    shipments = spark.read.format("delta").load(str(settings.SILVER / "shipments"))
    view = orders_in_flight(orders, shipments, as_of)
    view.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(settings.GOLD / "orders_in_flight"))
    out = Path("out")
    out.mkdir(exist_ok=True)
    rows = view.collect()
    with open(out / "orders_in_flight.csv", "w", encoding="utf-8") as f:
        f.write(",".join(view.columns) + "\n")
        for r in rows:
            f.write(",".join("" if v is None else str(v) for v in r) + "\n")
    print(f"{len(rows)} orders in flight as of {as_of}: {settings.GOLD}/orders_in_flight and out/orders_in_flight.csv")
    view.show(10, truncate=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m fernhill_move")
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("extract", help="step 2: every table into data/lake/raw with Spark JDBC")
    p.add_argument("--day", help="the extract's date (default: today)")
    p = commands.add_parser("recent", help="step 2: orders since a date, and the plan Spark runs")
    p.add_argument("--since", required=True)
    commands.add_parser("formats", help="step 3: the orders in each layout, and what each costs")
    p = commands.add_parser("register", help="step 4: create or update the Debezium connector")
    p.add_argument("--file", default=str(settings.CONNECTOR_FILE))
    p = commands.add_parser("status", help="step 4: the connector's state")
    p.add_argument("--name", default="fernhill-shop")
    p = commands.add_parser("connector-restart", help="step 7: restart the connector and its task (REST API)")
    p.add_argument("--name", default="fernhill-shop")
    commands.add_parser("topics", help="Kafka's topics and their end offsets")
    p = commands.add_parser("peek", help="records of a topic: peek fernhill.shop.orders --partition 2 --offset 744")
    p.add_argument("topic")
    p.add_argument("--partition", type=int, default=0)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("-n", type=int, default=1)
    commands.add_parser("groups", help="step 9: Kafka's consumer groups")
    p = commands.add_parser("reset-day", help="back to before make seed (source, topics, bronze/silver/gold)")
    p.add_argument("--name", default="fernhill-shop")
    p.add_argument("--yes", action="store_true")
    p = commands.add_parser("capture", help="step 4: the change events in Kafka, into a file")
    p.add_argument("--out", default="evidence/captured_events.jsonl")
    p = commands.add_parser("bronze", help="step 5: Kafka into bronze until caught up")
    p.add_argument("--out", default="evidence/bronze_progress.json")
    commands.add_parser("silver", help="steps 6 and 7: bronze into silver until caught up")
    p = commands.add_parser("show", help="steps 6 and 7: one row of silver: show orders 3257")
    p.add_argument("table", choices=TABLES)
    p.add_argument("key", nargs="+")
    commands.add_parser("contract", help="step 7: columns in bronze that tables.py does not name")
    p = commands.add_parser("reconcile", help="step 8: silver orders against a fresh batch snapshot")
    p.add_argument("--out", default="evidence/reconcile.json")
    p.add_argument("--version", type=int, help="read silver orders as of this Delta version (time travel)")
    p = commands.add_parser("lag", help="step 9: how far bronze is behind Kafka")
    p.add_argument("--out", default="evidence/lag.json")
    p = commands.add_parser("backfill", help="step 9: replay Kafka since a moment into silver")
    p.add_argument("--since", required=True, help="e.g. 2026-10-20T09:30:00+00:00")
    p.add_argument("--out", default="evidence/backfill.json")
    p = commands.add_parser("serve", help="step 10: the orders in flight for Customer Service")
    p.add_argument("--as-of", help="the day days_open counts to (default: today)")
    args = parser.parse_args(argv)
    try:
        globals()[f"cmd_{args.command.replace('-', '_')}"](args)
    except NotImplementedError as exc:
        print(f"Not written yet: {exc}", file=sys.stderr)
        return 1
    return 0
