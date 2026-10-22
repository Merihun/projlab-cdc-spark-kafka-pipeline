"""Step 10: what Customer Service looks at. Every order still on its way, with its parcel, from silver."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window  # noqa: F401
from pyspark.sql import functions as F  # noqa: F401

IN_FLIGHT = ("placed", "paid", "packed", "shipped")
COLUMNS = ("order_id", "customer_id", "order_date", "status", "carrier", "shipment_status", "days_open")


def orders_in_flight(orders: DataFrame, shipments: DataFrame, as_of: date) -> DataFrame:
    """Orders not deleted whose status is in IN_FLIGHT, with the carrier and status (as shipment_status) of
    their latest shipment that is not deleted (the highest shipment_id; null when there is none) and
    days_open, the days from order_date to `as_of`. Columns as COLUMNS; most days open first, then by
    order_id."""
    raise NotImplementedError("step 10: orders_in_flight")
