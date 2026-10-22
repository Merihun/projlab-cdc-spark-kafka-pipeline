"""Fernhill Outfitters' order system, from fixed seeds: every copy of the project gets the same rows and the
same day of traffic.

The database (PostgreSQL, schema `shop`) holds four tables: `customers`, `orders`, `order_items` and
`shipments`. `seed_rows()` is what they hold on Thursday 1 October 2026 at 08:00 UTC: two months of orders,
1 August to 30 September 2026. `plan()` is the day that follows, in five phases the steps run one at a time
(`python -m fernhill_shop traffic --phase NAME`). Each phase is a list of transactions; each transaction a
list of inserts, updates and deletes (and once, a new column), with the time it is committed on the
simulated clock.

What happens is planted on purpose, one thing per step that needs it:

- morning: new customers and orders, orders moving on (placed, paid, packed, shipped, delivered), shipments
  created, two customers changing their email, and one hard delete: the test order support placed by
  mistake on 30 September, deleted with its items.
- afternoon: more orders; orders that change several times in a row, once twice in one transaction; a
  shipment that goes to `exception` and is then corrected by the carrier's late scan, which carries an
  older `updated_at` than the row already has (the database applies it last all the same); an order whose
  item quantity changes.
- evening: a new column, `orders.sales_channel`, added while everything runs, then orders that use it, and
  older orders changing without it.
- night: price corrections on orders of 14 and 21 September, a fraud order of 21 September deleted, and the
  first orders of 2 October.
- burst: the carrier's overnight file (parcels shipped in September, delivered), the warehouse touching a
  fortnight of orders, and a wave of new orders: well over a thousand changes in a few seconds.

What the tables or the lake hold after each phase is worked out by the project's checks, not here.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from functools import cache

UTC = timezone.utc
SCHEMA = "shop"
FIRST_DAY = date(2026, 8, 1)
LAST_DAY = date(2026, 9, 30)
SEEDED_AT = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)
PHASES = ("morning", "afternoon", "evening", "night", "burst")
TABLES = ("customers", "orders", "order_items", "shipments")
KEYS = {
    "customers": ("customer_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "line_no"),
    "shipments": ("shipment_id",),
}
COLUMNS = {
    "customers": ("customer_id", "name", "email", "city", "created_at"),
    "orders": ("order_id", "customer_id", "order_date", "status", "total_amount", "updated_at"),
    "order_items": ("order_id", "line_no", "sku", "quantity", "unit_price"),
    "shipments": ("shipment_id", "order_id", "carrier", "status", "shipped_at", "delivered_at", "updated_at"),
}
NEW_COLUMN = ("orders", "sales_channel", "text")
STATUSES = ("placed", "paid", "packed", "shipped", "delivered", "cancelled")
NEXT_STATUS = {"placed": "paid", "paid": "packed", "packed": "shipped", "shipped": "delivered"}
CARRIERS = ("DPD", "Evri", "Royal Mail")
CHANNELS = ("web", "app", "marketplace")
CUSTOMERS = 400
FIRST_ORDER_ID = 1001
FIRST_SHIPMENT_ID = 5001
TEST_CUSTOMER_ID = CUSTOMERS
CORRECTION_DAYS = (date(2026, 9, 14), date(2026, 9, 21))

FIRST_NAMES = ("Aisha", "Ben", "Cara", "Dev", "Ellie", "Finn", "Grace", "Hamza", "Isla", "Jack", "Kofi", "Lena",
               "Mo", "Nina", "Oscar", "Priya", "Quinn", "Rosa", "Sam", "Tara", "Umar", "Vera", "Will", "Yara")
LAST_NAMES = ("Adeyemi", "Brown", "Chen", "Davies", "Evans", "Fraser", "Gill", "Hughes", "Iqbal", "Jones",
              "Kowalski", "Lewis", "Murphy", "Novak", "O'Brien", "Patel", "Quinn", "Reid", "Singh", "Taylor")
CITIES = ("Bristol", "Leeds", "Manchester", "Glasgow", "Cardiff", "London", "Belfast", "York", "Bath", "Dublin")
PRODUCTS = ("TENT", "BOOT", "JACK", "PACK", "SLEEP", "STOVE")


@dataclass(frozen=True)
class Op:
    """One change. insert: `row` is the whole row. update: `key` picks the row, `row` holds the new values of
    the columns that change. delete: `key` picks the row. add_column: `row` is {column: SQL type}."""

    kind: str
    table: str
    key: dict = field(default_factory=dict)
    row: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Tx:
    """One transaction: its changes, committed together at `at` on the simulated clock."""

    at: datetime
    ops: tuple[Op, ...]
    note: str


def _rng(*parts: object) -> random.Random:
    return random.Random(":".join(["fernhill-move-v1", *map(str, parts)]))


def money(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))


def at(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute, second), tzinfo=UTC)


@cache
def catalogue() -> tuple[tuple[str, Decimal], ...]:
    """The 48 SKUs on sale and their prices."""
    rng = _rng("catalogue")
    out = []
    for product in PRODUCTS:
        for n in range(1, 9):
            out.append((f"FH-{product}-{100 + n * 7:03d}", money(rng.choice((12, 18, 24, 35, 49, 65, 89, 129)) - 0.01)))
    return tuple(out)


def _customer(customer_id: int, rng: random.Random) -> dict:
    if customer_id == TEST_CUSTOMER_ID:
        name, email = "Support Test", "support-test@fernhill.example"
    else:
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        name = f"{first} {last}"
        email = f"{first}.{last.replace(chr(39), '')}{customer_id}@example.com".lower()
    created = at(date(2025, 1, 1) + timedelta(days=rng.randrange(570)), rng.randrange(7, 23), rng.randrange(60))
    return {"customer_id": customer_id, "name": name, "email": email, "city": rng.choice(CITIES), "created_at": created}


def _items(order_id: int, rng: random.Random) -> list[dict]:
    lines = rng.choices((1, 2, 3, 4), weights=(35, 35, 20, 10))[0]
    out = []
    for line_no in range(1, lines + 1):
        sku, price = rng.choice(catalogue())
        out.append({"order_id": order_id, "line_no": line_no, "sku": sku, "quantity": rng.choice((1, 1, 1, 2, 2, 3)),
                    "unit_price": price})
    return out


def total(items: list[dict]) -> Decimal:
    return money(sum(i["quantity"] * i["unit_price"] for i in items))


def _status_for_age(age: int, rng: random.Random) -> str:
    """Where an order placed `age` days before 1 October stands that morning."""
    if rng.random() < 0.03:
        return "cancelled"
    if age >= 7:
        return "delivered"
    if age >= 4:
        return rng.choices(("shipped", "delivered"), weights=(55, 45))[0]
    if age >= 2:
        return rng.choices(("packed", "shipped"), weights=(50, 50))[0]
    return rng.choices(("placed", "paid", "packed"), weights=(35, 45, 20))[0]


@cache
def seed_rows() -> dict[str, tuple[dict, ...]]:
    """Every row of the four tables on 1 October 2026 at 08:00 UTC, before any traffic."""
    rng = _rng("seed")
    customers = [_customer(i, rng) for i in range(1, CUSTOMERS + 1)]
    orders, items, shipments = [], [], []
    order_id, shipment_id = FIRST_ORDER_ID, FIRST_SHIPMENT_ID
    day = FIRST_DAY
    while day <= LAST_DAY:
        count = rng.randint(30, 40) + (8 if day.weekday() >= 5 else 0)
        for created in sorted(at(day, rng.randrange(7, 23), rng.randrange(60), rng.randrange(60)) for _ in range(count)):
            lines = _items(order_id, rng)
            age = (SEEDED_AT.date() - day).days
            status = _status_for_age(age, rng)
            updated = created + timedelta(hours=rng.randint(1, 20))
            shipped = created + timedelta(days=1, hours=rng.randint(2, 30))
            if status in ("shipped", "delivered") and shipped >= SEEDED_AT:
                status = "packed"
            if status in ("shipped", "delivered"):
                delivered = shipped + timedelta(days=rng.randint(1, 3), hours=rng.randint(0, 8)) if status == "delivered" else None
                if delivered and delivered >= SEEDED_AT:
                    status, delivered = "shipped", None
                updated = delivered or shipped
                shipments.append({"shipment_id": shipment_id, "order_id": order_id, "carrier": rng.choice(CARRIERS),
                                  "status": "delivered" if delivered else "in_transit", "shipped_at": shipped,
                                  "delivered_at": delivered, "updated_at": updated})
                shipment_id += 1
            updated = min(updated, SEEDED_AT - timedelta(minutes=1))
            orders.append({"order_id": order_id, "customer_id": rng.randint(1, CUSTOMERS - 1), "order_date": day,
                           "status": status, "total_amount": total(lines), "updated_at": updated})
            items.extend(lines)
            order_id += 1
        day += timedelta(days=1)
    # Support's test order, placed by mistake on the last evening: the morning deletes it.
    test_items = [{"order_id": order_id, "line_no": 1, "sku": catalogue()[0][0], "quantity": 1, "unit_price": catalogue()[0][1]}]
    orders.append({"order_id": order_id, "customer_id": TEST_CUSTOMER_ID, "order_date": LAST_DAY, "status": "placed",
                   "total_amount": total(test_items), "updated_at": at(LAST_DAY, 23, 41)})
    items.extend(test_items)
    return {"customers": tuple(customers), "orders": tuple(orders), "order_items": tuple(items),
            "shipments": tuple(shipments)}


def test_order_id() -> int:
    """The order support placed by mistake (customer `Support Test`), deleted in the morning."""
    return max(o["order_id"] for o in seed_rows()["orders"])


class _Book:
    """What the plan needs to know while it is written: each order's status and items, each shipment."""

    def __init__(self) -> None:
        rows = seed_rows()
        self.orders = {o["order_id"]: dict(o) for o in rows["orders"]}
        self.items: dict[int, list[dict]] = {}
        for i in rows["order_items"]:
            self.items.setdefault(i["order_id"], []).append(dict(i))
        self.shipments = {s["shipment_id"]: dict(s) for s in rows["shipments"]}
        self.shipment_of = {s["order_id"]: s["shipment_id"] for s in rows["shipments"]}
        self.next_order = max(self.orders) + 1
        self.next_shipment = max(self.shipments) + 1
        self.next_customer = CUSTOMERS + 1

    def new_order(self, when: datetime, rng: random.Random, channel: str | None = None) -> tuple[Op, ...]:
        order_id = self.next_order
        self.next_order += 1
        lines = _items(order_id, rng)
        order = {"order_id": order_id, "customer_id": rng.randint(1, self.next_customer - 1), "order_date": when.date(),
                 "status": "placed", "total_amount": total(lines), "updated_at": when}
        if channel is not None:
            order["sales_channel"] = channel
        self.orders[order_id] = dict(order)
        self.items[order_id] = [dict(i) for i in lines]
        return (Op("insert", "orders", row=order), *(Op("insert", "order_items", row=i) for i in lines))

    def advance(self, order_id: int, when: datetime, rng: random.Random) -> tuple[Op, ...]:
        """The order's next status, with its shipment created or delivered when that is the step."""
        status = NEXT_STATUS[self.orders[order_id]["status"]]
        self.orders[order_id]["status"] = status
        ops = [Op("update", "orders", {"order_id": order_id}, {"status": status, "updated_at": when})]
        if status == "shipped":
            shipment_id = self.next_shipment
            self.next_shipment += 1
            row = {"shipment_id": shipment_id, "order_id": order_id, "carrier": rng.choice(CARRIERS),
                   "status": "in_transit", "shipped_at": when, "delivered_at": None, "updated_at": when}
            self.shipments[shipment_id] = row
            self.shipment_of[order_id] = shipment_id
            ops.append(Op("insert", "shipments", row=row))
        elif status == "delivered" and order_id in self.shipment_of:
            shipment_id = self.shipment_of[order_id]
            self.shipments[shipment_id].update(status="delivered", delivered_at=when, updated_at=when)
            ops.append(Op("update", "shipments", {"shipment_id": shipment_id},
                          {"status": "delivered", "delivered_at": when, "updated_at": when}))
        return tuple(ops)

    def with_status(self, *statuses: str, days: tuple[date, date] | None = None) -> list[int]:
        return sorted(k for k, o in self.orders.items() if o["status"] in statuses and o["customer_id"] != TEST_CUSTOMER_ID
                      and (days is None or days[0] <= o["order_date"] <= days[1]))


def _clock(start: datetime, n: int, rng: random.Random, spread: tuple[int, int] = (60, 300)) -> list[datetime]:
    out, now = [], start
    for _ in range(n):
        now += timedelta(seconds=rng.randint(*spread))
        out.append(now)
    return out


def _morning(book: _Book) -> list[Tx]:
    rng = _rng("morning")
    jobs: list[tuple[str, object]] = []
    jobs += [("order", None)] * 30
    movers = rng.sample(book.with_status("placed", "paid", "packed", "shipped", days=(date(2026, 9, 24), LAST_DAY)), 40)
    jobs += [("advance", m) for m in movers]
    jobs += [("email", c) for c in rng.sample(range(1, CUSTOMERS), 2)]
    rng.shuffle(jobs)
    jobs.insert(0, ("customers", None))
    jobs.insert(17, ("delete-test-order", None))
    txs = []
    for when, (job, arg) in zip(_clock(at(date(2026, 10, 1), 8, 0), len(jobs), rng), jobs, strict=True):
        if job == "customers":
            rows = []
            for _ in range(3):
                row = _customer(book.next_customer, rng)
                row["created_at"] = when
                rows.append(row)
                book.next_customer += 1
            txs.append(Tx(when, tuple(Op("insert", "customers", row=r) for r in rows), "three new customers"))
        elif job == "order":
            ops = book.new_order(when, rng)
            txs.append(Tx(when, ops, f"new order {ops[0].row['order_id']}"))
        elif job == "advance":
            ops = book.advance(arg, when, rng)  # type: ignore[arg-type]
            txs.append(Tx(when, ops, f"order {arg} {ops[0].row['status']}"))
        elif job == "email":
            email = f"new.address{arg}@example.com"
            txs.append(Tx(when, (Op("update", "customers", {"customer_id": arg}, {"email": email}),), f"customer {arg} new email"))
        else:
            order_id = test_order_id()
            ops = tuple(Op("delete", "order_items", {"order_id": order_id, "line_no": i["line_no"]}) for i in book.items.pop(order_id))
            del book.orders[order_id]
            txs.append(Tx(when, (*ops, Op("delete", "orders", {"order_id": order_id})), f"test order {order_id} deleted"))
    return txs


def _afternoon(book: _Book, morning_orders: list[int]) -> list[Tx]:
    rng = _rng("afternoon")
    txs: list[Tx] = []
    clock = iter(_clock(at(date(2026, 10, 1), 12, 0), 200, rng, (40, 120)))
    runners = morning_orders[:10]
    double, changed_items = morning_orders[10], morning_orders[11]
    # The shipment the carrier gets wrong: in transit since 29 September.
    late = min(s for s, row in book.shipments.items() if row["status"] == "in_transit" and row["shipped_at"].date() == date(2026, 9, 29))
    jobs: list[tuple[str, object]] = [("order", None)] * 20 + [("advance", r) for r in runners for _ in range(3)]
    rng.shuffle(jobs)
    jobs.insert(5, ("double", double))
    jobs.insert(9, ("exception", late))
    jobs.insert(14, ("quantity", changed_items))
    jobs.append(("late-scan", late))
    for job, arg in jobs:
        when = next(clock)
        if job == "order":
            ops = book.new_order(when, rng)
            txs.append(Tx(when, ops, f"new order {ops[0].row['order_id']}"))
        elif job == "advance":
            ops = book.advance(arg, when, rng)  # type: ignore[arg-type]
            txs.append(Tx(when, ops, f"order {arg} {ops[0].row['status']}"))
        elif job == "double":
            first = book.advance(arg, when, rng)  # type: ignore[arg-type]
            second = book.advance(arg, when, rng)  # type: ignore[arg-type]
            txs.append(Tx(when, first + second, f"order {arg} paid and packed in one transaction"))
        elif job == "exception":
            row = book.shipments[arg]  # type: ignore[index]
            row.update(status="exception", updated_at=when)
            txs.append(Tx(when, (Op("update", "shipments", {"shipment_id": arg}, {"status": "exception", "updated_at": when}),),
                          f"shipment {arg} exception"))
        elif job == "late-scan":
            scanned = at(LAST_DAY, 16, 20)
            book.shipments[arg].update(status="delivered", delivered_at=scanned, updated_at=scanned)  # type: ignore[index]
            order_id = book.shipments[arg]["order_id"]  # type: ignore[index]
            book.orders[order_id]["status"] = "delivered"
            txs.append(Tx(when, (
                Op("update", "shipments", {"shipment_id": arg}, {"status": "delivered", "delivered_at": scanned, "updated_at": scanned}),
                Op("update", "orders", {"order_id": order_id}, {"status": "delivered", "updated_at": scanned}),
            ), f"shipment {arg} delivered on 30 September, scanned late"))
        else:
            item = book.items[arg][0]  # type: ignore[index]
            item["quantity"] += 1
            amount = total(book.items[arg])  # type: ignore[index]
            book.orders[arg]["total_amount"] = amount  # type: ignore[index]
            txs.append(Tx(when, (
                Op("update", "order_items", {"order_id": arg, "line_no": item["line_no"]}, {"quantity": item["quantity"]}),
                Op("update", "orders", {"order_id": arg}, {"total_amount": amount, "updated_at": when}),
            ), f"order {arg} quantity changed"))
    return txs


def _evening(book: _Book, morning_orders: list[int]) -> list[Tx]:
    rng = _rng("evening")
    clock = iter(_clock(at(date(2026, 10, 1), 17, 0), 100, rng, (60, 240)))
    column, sql_type = NEW_COLUMN[1], NEW_COLUMN[2]
    txs = [Tx(next(clock), (Op("add_column", "orders", row={column: sql_type}),), f"orders.{column} added")]
    jobs: list[tuple[str, object]] = [("order", None)] * 25
    jobs += [("advance", o) for o in rng.sample(book.with_status("packed", "shipped", days=(date(2026, 9, 26), LAST_DAY)), 15)]
    rng.shuffle(jobs)
    jobs.insert(12, ("cancel", morning_orders[12]))
    for job, arg in jobs:
        when = next(clock)
        if job == "order":
            ops = book.new_order(when, rng, channel=rng.choice(CHANNELS))
            txs.append(Tx(when, ops, f"new order {ops[0].row['order_id']} ({ops[0].row[column]})"))
        elif job == "advance":
            ops = book.advance(arg, when, rng)  # type: ignore[arg-type]
            txs.append(Tx(when, ops, f"order {arg} {ops[0].row['status']}"))
        else:
            book.orders[arg]["status"] = "cancelled"  # type: ignore[index]
            txs.append(Tx(when, (Op("update", "orders", {"order_id": arg}, {"status": "cancelled", "updated_at": when}),),
                          f"order {arg} cancelled"))
    return txs


def _night(book: _Book) -> list[Tx]:
    rng = _rng("night")
    clock = iter(_clock(at(date(2026, 10, 1), 22, 0), 100, rng, (300, 900)))
    txs = []
    for day, n in zip(CORRECTION_DAYS, (3, 2), strict=True):
        for order_id in rng.sample(book.with_status("delivered", days=(day, day)), n):
            when = next(clock)
            item = book.items[order_id][0]
            item["unit_price"] = money(item["unit_price"] * Decimal("0.9"))
            amount = total(book.items[order_id])
            book.orders[order_id]["total_amount"] = amount
            txs.append(Tx(when, (
                Op("update", "order_items", {"order_id": order_id, "line_no": item["line_no"]}, {"unit_price": item["unit_price"]}),
                Op("update", "orders", {"order_id": order_id}, {"total_amount": amount, "updated_at": when}),
            ), f"order {order_id} price corrected"))
    day = CORRECTION_DAYS[1]
    fraud = (book.with_status("cancelled", days=(day, day)) or book.with_status("delivered", days=(day, day))[-1:])[0]
    when = next(clock)
    ops = [Op("delete", "order_items", {"order_id": fraud, "line_no": i["line_no"]}) for i in book.items.pop(fraud)]
    if fraud in book.shipment_of:
        shipment_id = book.shipment_of.pop(fraud)
        del book.shipments[shipment_id]
        ops.append(Op("delete", "shipments", {"shipment_id": shipment_id}))
    del book.orders[fraud]
    txs.append(Tx(when, (*ops, Op("delete", "orders", {"order_id": fraud})), f"fraud order {fraud} deleted"))
    for _ in range(6):
        when = next(clock)
        if when.date() == date(2026, 10, 1):
            when = at(date(2026, 10, 2), 0, rng.randrange(5, 55))
        ops = book.new_order(when, rng, channel=rng.choice(CHANNELS))
        txs.append(Tx(when, ops, f"new order {ops[0].row['order_id']}"))
    return sorted(txs, key=lambda t: t.at)


def _burst(book: _Book) -> list[Tx]:
    rng = _rng("burst")
    start = at(date(2026, 10, 2), 6, 0)
    txs = []
    # The carrier's overnight file: every parcel shipped before 1 October and still in transit was delivered.
    delivered = [o for o in book.with_status("shipped") if o in book.shipment_of
                 and book.shipments[book.shipment_of[o]]["shipped_at"] < at(date(2026, 10, 1), 0)]
    for chunk in range(0, len(delivered), 50):
        when = start + timedelta(seconds=chunk // 50)
        ops: list[Op] = []
        for order_id in delivered[chunk:chunk + 50]:
            ops.extend(book.advance(order_id, when, rng))
        txs.append(Tx(when, tuple(ops), "carrier file"))
    # The warehouse system touches every delivered order of the last fortnight.
    touched = book.with_status("delivered", days=(date(2026, 9, 17), LAST_DAY))
    when = start + timedelta(minutes=1)
    for chunk in range(0, len(touched), 100):
        txs.append(Tx(when, tuple(Op("update", "orders", {"order_id": o}, {"updated_at": when}) for o in touched[chunk:chunk + 100]),
                      "warehouse re-sync"))
    # The morning wave of orders.
    for i in range(200):
        when = start + timedelta(minutes=2, seconds=i)
        ops = book.new_order(when, rng, channel=rng.choice(CHANNELS))
        txs.append(Tx(when, ops, f"new order {ops[0].row['order_id']}"))
    return txs


@cache
def plan() -> dict[str, tuple[Tx, ...]]:
    """The five phases of traffic, in order, as transactions."""
    book = _Book()
    morning = _morning(book)
    new_orders = [t.ops[0].row["order_id"] for t in morning if t.note.startswith("new order")]
    out = {"morning": morning, "afternoon": _afternoon(book, new_orders), "evening": _evening(book, new_orders),
           "night": _night(book), "burst": _burst(book)}
    return {phase: tuple(txs) for phase, txs in out.items()}


def changes_in(phase: str) -> int:
    """How many rows the phase inserts, updates or deletes (a new column is not a row change)."""
    return sum(1 for tx in plan()[phase] for op in tx.ops if op.kind != "add_column")
