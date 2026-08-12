-- payment_service schema (schema itself is created by Flyway via spring.flyway.create-schemas=true
-- and search_path is pinned to payment_service via spring.flyway.default-schema, so these DDL
-- statements are unqualified and land in the right place).
--
-- One row per payment attempt for an order. In this demo there is exactly one attempt per order
-- (no retries on failure), created in status PENDING the instant an ORDER_PLACED event is
-- consumed, then moved to COMPLETED or FAILED once the simulated gateway delay elapses.
CREATE TABLE payments (
    id              BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(40)     NOT NULL,
    user_id         VARCHAR(64)     NOT NULL,
    amount          NUMERIC(10, 2)  NOT NULL,
    status          VARCHAR(16)     NOT NULL, -- PENDING | COMPLETED | FAILED
    transaction_id  VARCHAR(64),               -- set only when status = COMPLETED
    failure_reason  VARCHAR(256),              -- set only when status = FAILED
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Supports GET /api/payments/order/{orderId} without a full table scan.
CREATE INDEX idx_payments_order_id ON payments (order_id);
