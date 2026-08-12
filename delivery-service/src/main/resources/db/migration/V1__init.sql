-- delivery_service schema: tracks driver assignment and delivery progress
-- for orders whose food has been PREPARED by kitchen-service.
CREATE TABLE IF NOT EXISTS deliveries (
    id            BIGSERIAL PRIMARY KEY,
    order_id      VARCHAR(40) NOT NULL,
    driver_id     VARCHAR(16),
    driver_name   VARCHAR(64),
    status        VARCHAR(16) NOT NULL,
    eta_minutes   INT,
    assigned_at   TIMESTAMPTZ,
    picked_up_at  TIMESTAMPTZ,
    enroute_at    TIMESTAMPTZ,
    delivered_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_deliveries_order_id ON deliveries (order_id);
