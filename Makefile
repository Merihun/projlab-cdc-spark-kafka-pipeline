help:  ## list the targets (the default: plain `make` changes nothing)
	@grep -E '^[a-z-]+:.*## ' Makefile | sed 's/:.*## /\t/'

.PHONY: help seed traffic source-status sql describe topics peek groups connector-restart reset-day extract recent formats register connector-status capture bronze silver contract show redeliver reconcile lag backfill serve test lint check

# The Python to run. On a Mac or Linux machine without a `python` command: make test PYTHON=python3
PYTHON ?= python
MOVE = $(PYTHON) -m fernhill_move

PHASE ?= morning
SINCE ?= 2026-09-24
TABLE ?= orders
KEY ?= 3257
OUT ?=
Q ?=
FILE ?=
TOPIC ?= fernhill.shop.orders
PARTITION ?= 0
OFFSET ?= 0
N ?= 1
VERSION ?=
YES ?=
FROM ?= $(shell cat evidence/burst_started_at.txt 2>/dev/null)

seed:  ## step 1: load the rows of 1 October 2026, 08:00 into PostgreSQL (once)
	$(PYTHON) -m fernhill_shop seed

traffic:  ## steps 4 to 9: run one phase of the day: make traffic PHASE=morning (afternoon, evening, night, burst)
	$(PYTHON) -m fernhill_shop traffic --phase $(PHASE)

source-status:  ## rows per table, the phases that have run, the replication slot
	$(PYTHON) -m fernhill_shop status

sql:  ## SQL on the order system: make sql Q="SHOW wal_level" or make sql FILE=sql/tour.sql
	$(PYTHON) -m fernhill_shop sql $(if $(FILE),--file $(FILE),"$(Q)")

describe:  ## a table's columns, primary key and replica identity: make describe TABLE=shop.orders
	$(PYTHON) -m fernhill_shop describe $(TABLE)

topics:  ## Kafka's topics and their end offsets
	$(MOVE) topics

peek:  ## records of a topic: make peek TOPIC=fernhill.shop.orders PARTITION=2 OFFSET=744 N=2
	$(MOVE) peek $(TOPIC) --partition $(PARTITION) --offset $(OFFSET) -n $(N)

groups:  ## step 9: Kafka's consumer groups
	$(MOVE) groups

connector-restart:  ## step 7: restart the connector and its task through Connect's REST API
	$(MOVE) connector-restart

reset-day:  ## start the day again: empties the shop, the fernhill topics, bronze/silver/gold: make reset-day YES=1
	$(MOVE) reset-day $(if $(YES),--yes)

extract:  ## step 2: every table into data/lake/raw with Spark JDBC
	$(MOVE) extract

recent:  ## step 2: orders since a date and the plan Spark runs: make recent SINCE=2026-09-24
	$(MOVE) recent --since $(SINCE)

formats:  ## step 3: the orders in each layout under data/lake/formats, and what each costs
	$(MOVE) formats

register:  ## step 4: create or update the Debezium connector from connector/fernhill-shop.json
	$(MOVE) register

connector-status:  ## step 4: the connector's state and its task's
	$(MOVE) status

capture:  ## step 4: the change events in Kafka into evidence/captured_events.jsonl
	$(MOVE) capture

bronze:  ## step 5: Kafka into bronze until caught up (writes evidence/bronze_progress.json)
	$(MOVE) bronze

silver:  ## steps 6 and 7: bronze into silver until caught up
	$(MOVE) silver

contract:  ## step 7: columns Debezium sends that tables.py does not name
	$(MOVE) contract

show:  ## steps 6 and 7: one row of silver: make show TABLE=shipments KEY=7032
	$(MOVE) show $(TABLE) $(KEY)

redeliver:  ## step 7: send the last 40 records of every orders and shipments partition again
	$(PYTHON) -m fernhill_shop redeliver

reconcile:  ## step 8: silver orders against a batch snapshot: make reconcile OUT=evidence/reconcile_before.json [VERSION=n]
	$(MOVE) reconcile --out $(or $(OUT),evidence/reconcile.json) $(if $(VERSION),--version $(VERSION))

lag:  ## step 9: how far bronze is behind Kafka (writes evidence/lag.json, or OUT=...)
	$(MOVE) lag --out $(or $(OUT),evidence/lag.json)

backfill:  ## step 9: replay Kafka since a moment into silver: make backfill FROM=2026-10-20T09:30:00+00:00
	$(MOVE) backfill --since $(FROM)

serve:  ## step 10: the orders in flight, to data/lake/gold and out/orders_in_flight.csv
	$(MOVE) serve

test:  ## your tests: tests/test_cdc.py (no lab needed)
	$(PYTHON) -m pytest -q tests/test_cdc.py

lint:  ## ruff on your code
	$(PYTHON) -m ruff check fernhill_move tests

check:  ## the project's checks on your machine: needs the local check kit in .projlabs/ (Review page)
	$(PYTHON) .projlabs/check.py
