-- Runs once, when the postgres volume is first created (and again after `docker compose down -v`).
-- Fernhill Outfitters' order system: the tables Debezium captures. `python -m fernhill_shop seed` fills them.
CREATE SCHEMA shop;

CREATE TABLE shop.customers (
    customer_id  integer     PRIMARY KEY,
    name         text        NOT NULL,
    email        text        NOT NULL,
    city         text        NOT NULL,
    created_at   timestamptz NOT NULL
);

CREATE TABLE shop.orders (
    order_id     integer       PRIMARY KEY,
    customer_id  integer       NOT NULL,
    order_date   date          NOT NULL,
    status       text          NOT NULL,
    total_amount numeric(10,2) NOT NULL,
    updated_at   timestamptz   NOT NULL
);

CREATE TABLE shop.order_items (
    order_id     integer       NOT NULL,
    line_no      integer       NOT NULL,
    sku          text          NOT NULL,
    quantity     integer       NOT NULL,
    unit_price   numeric(10,2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE shop.shipments (
    shipment_id  integer     PRIMARY KEY,
    order_id     integer     NOT NULL,
    carrier      text        NOT NULL,
    status       text        NOT NULL,
    shipped_at   timestamptz NOT NULL,
    delivered_at timestamptz,
    updated_at   timestamptz NOT NULL
);

-- A delete or an update then carries the whole old row in Debezium's `before`, not only the key.
ALTER TABLE shop.customers REPLICA IDENTITY FULL;
ALTER TABLE shop.orders REPLICA IDENTITY FULL;
ALTER TABLE shop.order_items REPLICA IDENTITY FULL;
ALTER TABLE shop.shipments REPLICA IDENTITY FULL;

-- The traffic simulator's own notes: which phases have run. Not captured.
CREATE TABLE shop.simulator_log (
    phase    text        PRIMARY KEY,
    changes  integer     NOT NULL,
    ran_at   timestamptz NOT NULL DEFAULT now()
);
