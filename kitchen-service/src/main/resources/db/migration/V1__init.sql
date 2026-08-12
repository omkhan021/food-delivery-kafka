-- kitchen_service schema init (schema itself is created by Flyway via
-- spring.flyway.create-schemas=true / spring.flyway.schemas=kitchen_service)

CREATE TABLE IF NOT EXISTS kitchen_orders (
    id                BIGSERIAL PRIMARY KEY,
    order_id          VARCHAR(40) NOT NULL,
    status            VARCHAR(16) NOT NULL,
    estimated_minutes INT,
    received_at       TIMESTAMPTZ,
    preparing_at      TIMESTAMPTZ,
    prepared_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One kitchen_orders row per order flowing through this service's state machine.
CREATE UNIQUE INDEX IF NOT EXISTS uq_kitchen_orders_order_id ON kitchen_orders (order_id);
CREATE INDEX IF NOT EXISTS idx_kitchen_orders_status ON kitchen_orders (status);
