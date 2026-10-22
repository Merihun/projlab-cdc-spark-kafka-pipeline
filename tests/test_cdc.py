"""Your tests of fernhill_move/cdc.py: plain Python, no lab needed. Run them with make test.

Two are written for you, as worked examples: the first passes once parse_change is written (step 4), the
second is skipped until latest_per_key is (step 6). Add your own below them as you go.
"""

from datetime import date
from decimal import Decimal

import pytest

from fernhill_move.cdc import Change, latest_per_key, parse_change

# A real update event from the lab (fernhill.shop.orders), trimmed to the fields that matter.
KEY = '{"order_id":3035}'
VALUE = (
    '{"before":{"order_id":3035,"customer_id":97,"order_date":20721,"status":"shipped","total_amount":"35.98",'
    '"updated_at":"2026-09-27T01:45:53.000000Z"},"after":{"order_id":3035,"customer_id":97,"order_date":20721,'
    '"status":"delivered","total_amount":"35.98","updated_at":"2026-10-01T08:11:04.000000Z"},'
    '"source":{"table":"orders","lsn":27764440,"ts_ms":1792510792742},"op":"u","ts_ms":1792510793144}'
)


def test_an_update_is_its_after_image():
    change = parse_change(KEY, VALUE)
    assert (change.table, change.op, change.key, change.lsn) == ("orders", "u", (3035,), 27764440)
    assert change.row["status"] == "delivered"
    assert change.row["order_date"] == date(2026, 9, 25)
    assert change.row["total_amount"] == Decimal("35.98")


def test_the_highest_lsn_wins_whatever_the_order():
    row = {"order_id": 1, "status": "placed"}
    older = Change("orders", "c", (1,), row, lsn=10, source_ts_ms=1)
    newer = Change("orders", "u", (1,), {**row, "status": "paid"}, lsn=20, source_ts_ms=2)
    try:
        got = latest_per_key([newer, older])
    except NotImplementedError:
        pytest.skip("latest_per_key is step 6's")
    assert got == [newer]
