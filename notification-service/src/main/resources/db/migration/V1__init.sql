-- notification_service schema (created automatically by Flyway via spring.flyway.create-schemas=true)

CREATE TABLE IF NOT EXISTS notifications (
    id           BIGSERIAL PRIMARY KEY,
    order_id     VARCHAR(40),
    source_topic VARCHAR(64) NOT NULL,
    event_type   VARCHAR(64) NOT NULL,
    message      VARCHAR(256) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supports "list, newest first" and "by order id" queries efficiently.
CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_order_id ON notifications (order_id);
