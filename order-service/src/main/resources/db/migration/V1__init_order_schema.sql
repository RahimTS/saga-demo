CREATE TABLE IF NOT EXISTS order_schema.orders (
    id                      UUID PRIMARY KEY,
    customer_id             VARCHAR(100) NOT NULL,
    product_id              VARCHAR(100) NOT NULL,
    quantity                INTEGER NOT NULL,
    amount                  NUMERIC(19,2) NOT NULL,
    status                  VARCHAR(50) NOT NULL,  -- PENDING, COMPLETED, CANCELLED
    force_payment_failure   BOOLEAN DEFAULT FALSE,
    force_stock_failure     BOOLEAN DEFAULT FALSE,
    created_at              TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS order_schema.processed_events (
    event_id        VARCHAR(100) PRIMARY KEY,   -- UUID from incoming event
    processed_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS order_schema.dead_letter_events (
    id              BIGSERIAL PRIMARY KEY,
    topic           VARCHAR(200) NOT NULL,
    payload         TEXT NOT NULL,              -- Raw JSON of the failed message
    error_message   TEXT,
    failed_at       TIMESTAMP NOT NULL
);