CREATE TABLE IF NOT EXISTS inventory_schema.inventory (
    id              UUID PRIMARY KEY,
    product_id      VARCHAR(100) NOT NULL UNIQUE,
    quantity        INTEGER NOT NULL,
    reserved        INTEGER NOT NULL DEFAULT 0,    -- how much is currently reserved
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_schema.processed_events (
    event_id        VARCHAR(100) PRIMARY KEY,
    processed_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_schema.dead_letter_events (
    id              BIGSERIAL PRIMARY KEY,
    topic           VARCHAR(200) NOT NULL,
    payload         TEXT NOT NULL,
    error_message   TEXT,
    failed_at       TIMESTAMP NOT NULL
);

-- Seed data — these product IDs can be used in Postman requests
INSERT INTO inventory_schema.inventory (id, product_id, quantity, reserved, updated_at)
VALUES
    (gen_random_uuid(), 'prod-001', 100, 0, NOW()),
    (gen_random_uuid(), 'prod-002', 5,  0, NOW()),   -- low stock, easy to exhaust
    (gen_random_uuid(), 'prod-003', 0,  0, NOW());   -- out of stock immediately