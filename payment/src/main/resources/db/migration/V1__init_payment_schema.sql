CREATE TABLE IF NOT EXISTS payment_schema.payments (
    id              UUID PRIMARY KEY,
    order_id        VARCHAR(100) NOT NULL,
    customer_id     VARCHAR(100) NOT NULL,
    amount          NUMERIC(19,2) NOT NULL,
    status          VARCHAR(50) NOT NULL,    -- COMPLETED, FAILED
    failure_reason  VARCHAR(255),
    created_at      TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS payment_schema.processed_events (
    event_id        VARCHAR(100) PRIMARY KEY,
    processed_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS payment_schema.dead_letter_events (
    id              BIGSERIAL PRIMARY KEY,
    topic           VARCHAR(200) NOT NULL,
    payload         TEXT NOT NULL,
    error_message   TEXT,
    failed_at       TIMESTAMP NOT NULL
);