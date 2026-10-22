# Orders on the Move: Data at Rest and in Motion with Spark, Kafka and CDC

What this starter gives you, where things are, and the rules every check compares against.

Fernhill Outfitters' orders live in PostgreSQL. Finance wants every day's orders kept in the lake, as history;
Customer Service wants to see orders as they change, not tomorrow. Today both get a CSV someone exports by
hand. You build both paths: a batch extract with Spark JDBC (data at rest), and change data capture with
Debezium, Kafka and Spark Structured Streaming into Delta tables (data in motion), and you prove the two agree.

| Provided | What it is |
|----------|------------|
| `compose.yaml` | The lab: `workspace` (Python 3.12, Java 17, PySpark 3.5, Delta Lake), `postgres` (the order system, with logical replication), `kafka` (one broker) and `connect` (Kafka Connect with Debezium). |
| `db/init/01-shop.sql` | The order system's tables, created when PostgreSQL first starts. |
| `fernhill_shop/` | The order system's rows and a day of traffic. You run it; you do not change it. |
| `fernhill_move/` | Your package, one module per step. Each function you write says what it does and raises `NotImplementedError` until you write it. `cli.py`, `settings.py`, `spark.py` and `tables.py` are provided. |
| `connector/fernhill-shop.json` | The Debezium connector's configuration, with `...` where you fill in values (step 4). |
| `answers/` | Your answers: `source.txt`, `extract.txt`, `formats.txt`, `real_life.txt`, `reconcile.md`, `runbook.md`, `serving.txt`. |
| `evidence/` | Files the commands write from your lab, which you commit: the checks compare them with what a correct run gives. |
| `tests/test_environment.py` | Proves the lab is up. `tests/test_cdc.py`: your tests, two written for you (`make test`). |
| `Makefile` | One target per command (`make` alone lists them). |

## Where to run things

Start the lab from this folder on your computer: `docker compose up -d --build --wait` (the first build and
the first Spark session download a few hundred MB). In GitHub Codespaces or a VS Code Dev Container the lab
starts by itself.

**Everything in this project runs in the workspace terminal**: in Codespaces or VS Code, Terminal → New
Terminal; with plain Docker, `docker compose exec workspace bash`. You are in `/workspace`, this folder.
Check the lab with `python -m pytest -q tests/test_environment.py`: 6 passed. The workspace has no `psql`,
no Docker and no Kafka scripts, so the kit gives you its own tools for them:

| You want | In the workspace terminal | Only on your own computer (plain Docker), if you prefer |
|----------|---------------------------|----------------------------------------------------------|
| SQL on the order system | `make sql Q="SHOW wal_level"`, `make sql FILE=sql/tour.sql` | `docker compose exec postgres psql -U projlab` |
| A table's columns and key | `make describe TABLE=shop.orders` | `\d shop.orders` in psql |
| Kafka's topics and end offsets | `make topics` | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list` |
| Records of a topic | `make peek TOPIC=fernhill.shop.orders PARTITION=0 OFFSET=0 N=1` | `kafka-console-consumer.sh` in the kafka container |
| Consumer groups | `make groups` | `kafka-consumer-groups.sh --list` in the kafka container |
| Restart the connector | `make connector-restart` (Connect's REST API) | `docker compose restart connect` (the whole worker) |

Kafka Connect's REST API is at `http://connect:8083` from the workspace (`curl -s http://connect:8083/`).
Nothing but JupyterLab (8888) and the Spark UI (4040) is published to your computer.

Every Spark command first prints Ivy's resolution report (about 50 lines: `:: loading settings`, `found ...`,
a table of modules) and a few warnings (`NativeCodeLoader`, `SparkStringUtils`); its own output comes after
them. The first one (`make extract`) also downloads Delta Lake, the Kafka connector and the PostgreSQL JDBC
driver from Maven Central into `data/ivy` (git-ignored), in this folder, so a rebuilt workspace doesn't
download them again.

## The order system

PostgreSQL 16, database `projlab`, schema `shop`, user `projlab` (password `projlab`; local only). It runs with
`wal_level=logical`, so its write-ahead log can be decoded into row changes, and every table has
`REPLICA IDENTITY FULL`, so an update or delete carries the whole old row.

| Table | Key | Columns |
|-------|-----|---------|
| `customers` | `customer_id` | `name`, `email`, `city`, `created_at` (timestamptz) |
| `orders` | `order_id` | `customer_id`, `order_date` (date), `status`, `total_amount` (numeric(10,2)), `updated_at` (timestamptz) |
| `order_items` | `order_id`, `line_no` | `sku`, `quantity`, `unit_price` (numeric(10,2)) |
| `shipments` | `shipment_id` | `order_id`, `carrier`, `status` (`in_transit`, `exception`, `delivered`), `shipped_at`, `delivered_at`, `updated_at` |

An order's `status` moves `placed` → `paid` → `packed` → `shipped` → `delivered`, or becomes `cancelled`.

## The day: `python -m fernhill_shop`

| Command | Step | What it does |
|---------|------|--------------|
| `make seed` | 1 | Loads the tables as they were on Thursday 1 October 2026 at 08:00 UTC: two months of orders (1 August to 30 September). Once. |
| `make traffic PHASE=morning` | 4 | New customers and orders, orders moving on, shipments, two new emails, and one hard delete: support's test order of 30 September (customer 400, "Support Test") and its item. |
| `make traffic PHASE=afternoon` | 6 | More orders; orders changing several times in a row, once twice in one transaction (order 3257). Shipment 7032 goes to `exception`; later the carrier's late scan changes it again, with an `updated_at` of 30 September 16:20, older than the one the row already has. |
| `make redeliver` | 7 | Not the database: Kafka. Sends the last 40 records of every partition of `fernhill.shop.orders` and `fernhill.shop.shipments` again, as a connector restarting after a crash does. |
| `make traffic PHASE=evening` | 7 | `ALTER TABLE shop.orders ADD COLUMN sales_channel text`, then new orders with a channel (`web`, `app`, `marketplace`) and older orders changing without one. |
| `make traffic PHASE=night` | 8 | Price corrections on orders of 14 and 21 September, a fraud order of 21 September deleted with its items and shipment, the first orders of 2 October. |
| `make traffic PHASE=burst` | 9 | Over a thousand changes in a few seconds: the carrier's overnight file, the warehouse touching a fortnight of orders, 200 new orders. |

Each phase runs once, in this order, and only after the connector exists (its replication slot
`fernhill_slot`): otherwise its changes would never reach Kafka. `make source-status` shows the rows, the
phases that have run and the slot.

Debezium writes a phase's changes to Kafka a few seconds after the phase ends. `make capture`, `make bronze`
and `make lag` therefore wait first until the four topics' end offsets have not moved for 6 seconds
(`waiting for Debezium to catch up ...`).

**To start the day again** (a step went wrong, or you ran something out of order): `make reset-day YES=1`
stops and deletes the connector with its saved offsets, empties the shop (rows, the new column, the
phases, the slot), deletes the `fernhill.shop.*` topics and removes bronze, silver, gold and the
checkpoints (the raw extracts and formats stay). Then `make seed`, `make register`, and the traffic and
commands of steps 4 onwards again, in order. On your own computer, `docker compose down -v` and up does the
same and more.

## The lake

Everything lands under `data/lake` (git-ignored); checkpoints under `data/checkpoints`.

| Path | Format | Written by |
|------|--------|------------|
| `data/lake/raw/<table>/extracted_on=YYYY-MM-DD/` | Parquet | `make extract` (step 2): a whole table per day |
| `data/lake/formats/<layout>/` | Parquet, Delta | `make formats` (step 3) |
| `data/lake/bronze/changes/` | Delta | `make bronze` (step 5): every Kafka record, once |
| `data/lake/silver/<table>/` | Delta | `make silver` (steps 6 and 7): each row as PostgreSQL has it |
| `data/lake/gold/orders_in_flight/` | Delta | `make serve` (step 10), also `out/orders_in_flight.csv` |

## Step 2: the JDBC read

`jdbc_options(table, ...)` returns `url` (`jdbc_url()`: `jdbc:postgresql://HOST:PORT/DB` from
`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`), `dbtable` (`shop.<table>`), `user`, `password`, `driver`
(`org.postgresql.Driver`) and `fetchsize` (`1000`). With a partition column it adds `partitionColumn`,
`lowerBound`, `upperBound` and `numPartitions`: Spark then sends `numPartitions` queries side by side, each over
one stride of the column between the bounds. The bounds do not filter: rows outside them land in the first or
last query. Every value is a string.

## Step 3: layouts

`write_layout(df, path, layout, codec)` writes the orders, replacing what is at `path`:

| Layout | Format | Folders | Files |
|--------|--------|---------|-------|
| `flat` | Parquet | none | 1 |
| `by_day` | Parquet | `order_date=YYYY-MM-DD` | 1 per folder |
| `by_month` | Parquet | `order_month=YYYY-MM` (a column added from `order_date`) | 1 per folder |
| `delta_by_month` | Delta | `order_month=YYYY-MM` | 1 per folder |

`file_report(path)` counts the data files (`*.parquet`, not inside `_delta_log`), their bytes, the partition
folders (`name=value`) that hold them, and `avg_kb`, the average file size in KB (1 KB = 1024 bytes), rounded to
one decimal. `make formats` writes `flat` three times (`snappy`, `gzip`, `zstd`), the other layouts once with
`snappy`, and `delta_by_month` a second time, then prints a report.

## Step 4: the connector

`connector/fernhill-shop.json` is `{"name": "fernhill-shop", "config": {...}}`. The config uses the
`pgoutput` plugin, `topic.prefix` `fernhill` (so the topics are `fernhill.shop.<table>`), the four tables in
`table.include.list` (`shop.customers,shop.orders,shop.order_items,shop.shipments`), `slot.name`
`fernhill_slot`, `decimal.handling.mode` `string`, and JSON without schemas for keys and values
(`key.converter.schemas.enable` and `value.converter.schemas.enable` `false`). The database settings are
placeholders, `${POSTGRES_HOST}` and so on, `${POSTGRES_PASSWORD}` included: `load_config` fills them in from the
environment, so the file holds no secret. `register` sends `PUT /connectors/fernhill-shop/config`, which
creates the connector or updates it: safe to run twice.

A Debezium record's key is the row's key (`{"order_id":3257}`); its value is the change:

    {"before": {...} or null, "after": {...} or null,
     "source": {..., "table": "orders", "lsn": 27862440, "ts_ms": 1792512564176, ...},
     "op": "c", "ts_ms": ...}

| `op` | Meaning | `before` | `after` | Action |
|------|---------|----------|---------|--------|
| `r` | read by the snapshot, when the connector starts | null | the row | upsert |
| `c` | insert | null | the row | upsert |
| `u` | update | the old row | the new row | upsert |
| `d` | delete | the old row | null | delete |

After each delete Debezium sends a **tombstone**: the same key with a null value, so that Kafka's log
compaction can forget the key. `source.lsn` is the change's position in PostgreSQL's log: later changes have
higher LSNs, and every row of the snapshot has the snapshot's LSN. `source.ts_ms` is when it was committed.

How a value travels in the JSON (`tables.WIRE`): a `date` as days since 1970-01-01 (`20727` is 2026-10-01), a
`numeric` as a string (`"129.95"`), a `timestamptz` as an ISO string in UTC (`"2026-10-01T08:07:26.000000Z"`).
`decode` turns them back: a `date`, a `Decimal`, an aware `datetime`. `parse_change(key, value)` returns a
`Change(table, op, key, row, lsn, source_ts_ms)`: `table` from `source.table`, `key` a tuple of the values of
`KEYS[table]` in the record's key, `row` the decoded `after` (for a delete, the decoded `before`); `None` for a
tombstone.

`make capture` writes `evidence/captured_events.jsonl`: every record in the four topics except the snapshot's,
plus the first snapshot record of each topic.

## Step 5: bronze

`kafka_options(bootstrap)`: `kafka.bootstrap.servers`, `subscribePattern` `fernhill\.shop\..*` (a regular
expression: escape the dots) and `startingOffsets` `earliest`. The bronze table keeps every record, once,
with these columns in this order:

| Column | Type | |
|--------|------|-|
| `topic`, `partition`, `offset` | string, int, bigint | where the record is in Kafka |
| `kafka_ts` | timestamp | the record's Kafka timestamp |
| `key`, `value` | string | the record as text; `value` is null for a tombstone |
| `table_name` | string | the topic's last part: `orders` |
| `op` | string | `$.op` of the value; null for a tombstone |
| `lsn`, `source_ts_ms` | bigint | `$.source.lsn`, `$.source.ts_ms`; null for a tombstone |

`start_bronze` appends to bronze with a checkpoint and `trigger(availableNow=True)`: it reads what is new
since the checkpoint and stops. `make bronze` writes the run's last progress report to
`evidence/bronze_progress.json`.

## Steps 6 and 7: silver, the rules

Silver holds one row per key and table: the table's columns (`tables.COLUMNS`, typed as there), then
`_deleted` (boolean), `_lsn` and `_source_ts_ms` (bigint).

1. A micro-batch's events are parsed per table: the columns come from `after`, or from `before` for a
   delete; tombstones are dropped.
2. Per key, only the event with the **highest LSN** counts. Not the order of arrival (a micro-batch holds
   several partitions, and a record can come again after newer ones), and never a business time such as
   `updated_at` (the late scan of shipment 7032 is the last change although its `updated_at` is older).
3. MERGE into the table: a key silver doesn't have is inserted; a key it has is updated only when the event's
   LSN is higher than the row's `_lsn`. So a batch merged twice, records delivered again, or an older window
   replayed later change nothing.
4. A delete is kept: the row's last image with `_deleted = true`. It remembers the key was deleted, so an old
   update replayed later can't bring the row back. Readers leave out `_deleted` rows.
5. A new column (`orders.sales_channel`, step 7) is added to the table by the MERGE (schema evolution:
   `spark.databricks.delta.schema.autoMerge.enabled`), once `tables.py` names it. `make contract` lists the
   columns Debezium sends that `tables.py` does not name.

`cdc.latest_per_key` and `cdc.apply_changes` are rules 2 to 4 in plain Python, on `Change`s: `apply_changes`
returns `{(table, key): Change}`, where a deleted key holds its `d` change.

## Step 8: reconciliation

Silver orders without `_deleted` rows (the stream) and a fresh JDBC read of `shop.orders` (the batch) are
summarised per `order_date`: `orders` (count), `amount` (sum of `total_amount`) and `checksum`, the sum of
each order's CRC-32 of `order_id|customer_id|status|total_amount` (as in `1001|59|delivered|47.97`), modulo
2**32. A day differs when any of the three differs; its kind is the first that applies of
`missing_in_stream` (only the batch has the day), `missing_in_batch`, `count`, `amount`, `content` (same count
and amount, but a status or customer changed). `make reconcile` writes `evidence/reconcile.json`
(`OUT=...` for another file).

## Step 9: lag and replay

A Spark query doesn't commit offsets to a Kafka consumer group: it keeps them in its checkpoint.
`<checkpoint>/offsets/<batch>` has three lines: `v1`, the batch's metadata (JSON), and the Kafka offsets it
read up to (JSON, `{"topic": {"partition": offset}}`, each the next offset to read); `<checkpoint>/commits/<batch>`
exists once the batch is done. The **lag** of a partition is its end offset (Kafka's high watermark) minus
the offset bronze has processed (the newest committed batch's). `make lag` writes `evidence/lag.json`.

A **replay** reads Kafka again from a moment: `startingOffsetsByTimestamp` gives, per topic and partition, a
time in epoch milliseconds, and Kafka finds the first offset at or after it;
`startingOffsetsByTimestampStrategy` `latest` makes a partition with nothing that new start at its end, and
`endingOffsets` `latest` stops the read at the end. `make backfill FROM=<time>` replays into silver and writes
`evidence/backfill.json` with the rows the MERGEs inserted, updated and deleted (`merge_metrics`, from each
table's history).

## Step 10: orders in flight

Orders not deleted whose status is `placed`, `paid`, `packed` or `shipped`, each with the `carrier` and
`status` (as `shipment_status`) of its latest shipment that is not deleted (the highest `shipment_id`; null
when there is none), and `days_open`,
the days from `order_date` to the day asked; most days open first, then by `order_id`.

## Commands

`make` lists every target. Tools: `sql`, `describe`, `topics`, `peek`, `groups`, `source-status`,
`connector-status`, `connector-restart`, `reset-day`. In order of the steps: `seed`, `extract`, `recent SINCE=2026-09-24`, `formats`, `register`,
`connector-status`, `traffic PHASE=...`, `capture`, `bronze`, `silver`, `show TABLE=orders KEY=3257`,
`redeliver`, `contract`, `reconcile`, `lag`, `backfill FROM=...`, `serve`; and `test`, `lint`, `check` (the
project's checks on your machine, with the local check kit from the Review page in `.projlabs/`).
