"""The four tables Debezium captures: their keys, and each column with the type it has in the lake.

Provided, and yours to keep up to date: when the order system adds a column, it belongs here (step 7).
"""

TABLES = ("customers", "orders", "order_items", "shipments")

KEYS = {
    "customers": ("customer_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "line_no"),
    "shipments": ("shipment_id",),
}

COLUMNS = {
    "customers": (("customer_id", "INT"), ("name", "STRING"), ("email", "STRING"), ("city", "STRING"),
                  ("created_at", "TIMESTAMP")),
    "orders": (("order_id", "INT"), ("customer_id", "INT"), ("order_date", "DATE"), ("status", "STRING"),
               ("total_amount", "DECIMAL(10,2)"), ("updated_at", "TIMESTAMP")),
    "order_items": (("order_id", "INT"), ("line_no", "INT"), ("sku", "STRING"), ("quantity", "INT"),
                    ("unit_price", "DECIMAL(10,2)")),
    "shipments": (("shipment_id", "INT"), ("order_id", "INT"), ("carrier", "STRING"), ("status", "STRING"),
                  ("shipped_at", "TIMESTAMP"), ("delivered_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")),
}

# How Debezium writes a value of each lake type in its JSON (with decimal.handling.mode = string):
# a DATE as the number of days since 1970-01-01, a DECIMAL as a string ("129.95"), a TIMESTAMP (PostgreSQL
# timestamptz) as an ISO 8601 string in UTC ("2026-10-01T08:07:26.000000Z").
WIRE = {"INT": "INT", "STRING": "STRING", "DATE": "INT", "DECIMAL(10,2)": "STRING", "TIMESTAMP": "STRING"}

TOPIC_PREFIX = "fernhill"


def topic(table: str) -> str:
    """The Kafka topic Debezium writes a table's changes to: fernhill.shop.orders for orders."""
    return f"{TOPIC_PREFIX}.shop.{table}"
