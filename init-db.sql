-- Create three schemas, one per service
CREATE SCHEMA IF NOT EXISTS order_schema;
CREATE SCHEMA IF NOT EXISTS inventory_schema;
CREATE SCHEMA IF NOT EXISTS payment_schema;

-- Grant all to saga user
GRANT ALL ON SCHEMA order_schema TO saga;
GRANT ALL ON SCHEMA inventory_schema TO saga;
GRANT ALL ON SCHEMA payment_schema TO saga;