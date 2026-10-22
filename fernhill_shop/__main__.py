"""python -m fernhill_shop seed | traffic --phase NAME | status | redeliver [--last N] | sql | describe TABLE"""

import argparse
import json

from fernhill_shop import data, db


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m fernhill_shop", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("seed", help="load the rows of 1 October 2026, 08:00 (step 1)")
    traffic = commands.add_parser("traffic", help="run one phase of the day's traffic")
    traffic.add_argument("--phase", required=True, choices=data.PHASES)
    traffic.add_argument("--pause", type=float, default=0.05, help="seconds between transactions")
    commands.add_parser("status", help="rows per table, the phases that have run, the replication slot")
    again = commands.add_parser("redeliver", help="re-send the last records of the orders and shipments topics (step 7)")
    again.add_argument("--last", type=int, default=40, help="records per partition")
    sql = commands.add_parser("sql", help="run SQL on the order system and print the rows (like psql)")
    sql.add_argument("query", nargs="?", help="one statement, e.g. \"SHOW wal_level\"")
    sql.add_argument("--file", help="a file of statements, e.g. sql/tour.sql")
    table = commands.add_parser("describe", help="a table's columns, primary key and replica identity (like \\d)")
    table.add_argument("table", help="e.g. shop.orders")
    commands.add_parser("reset", help="empty the shop (used by make reset-day)")
    args = parser.parse_args()

    if args.command == "redeliver":
        from fernhill_shop.kafka import redeliver

        sent = redeliver(args.last)
        for topic, n in sent.items():
            print(f"re-sent {n} records to {topic}")
        return 0
    with db.connect() as conn:
        if args.command == "seed":
            counts = db.seed(conn)
            print("Seeded schema shop: " + ", ".join(f"{t} {n:,}" for t, n in counts.items()))
        elif args.command == "traffic":
            changes = db.run_phase(conn, args.phase, args.pause)
            print(f"{args.phase}: {len(data.plan()[args.phase])} transactions, {changes:,} row changes committed")
        elif args.command == "sql":
            if not args.query and not args.file:
                raise SystemExit("Give a statement (make sql Q=\"SHOW wal_level\") or a file (make sql FILE=sql/tour.sql)")
            text = args.query if args.query else open(args.file, encoding="utf-8").read()
            print("\n\n".join(db.run_sql(conn, text)))
        elif args.command == "describe":
            print(db.describe(conn, args.table))
        elif args.command == "reset":
            db.reset(conn)
            print("The shop is empty: no rows, no phases, no replication slot. Next: make seed")
        else:
            print(json.dumps(db.status(conn), indent=2))
    return 0


raise SystemExit(main())
