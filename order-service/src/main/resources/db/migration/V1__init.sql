-- order_service schema
-- Managed exclusively by Flyway. Hibernate is configured with ddl-auto=validate and must never
-- create/alter these tables itself; this file is the single source of truth for table shape,
-- copied verbatim from ARCHITECTURE.md section 6.

CREATE TABLE orders (
  order_id VARCHAR(40) PRIMARY KEY,
  user_id VARCHAR(64) NOT NULL,
  payment_amount NUMERIC(10,2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  order_date DATE NOT NULL,
  order_time TIME NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
  id BIGSERIAL PRIMARY KEY,
  order_id VARCHAR(40) NOT NULL REFERENCES orders(order_id),
  item_name VARCHAR(128) NOT NULL,
  quantity INT NOT NULL,
  price NUMERIC(10,2) NOT NULL
);

CREATE TABLE order_status_history (
  id BIGSERIAL PRIMARY KEY,
  order_id VARCHAR(40) NOT NULL REFERENCES orders(order_id),
  status VARCHAR(32) NOT NULL,
  source_topic VARCHAR(64),
  note VARCHAR(256),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Audit / replay-visibility log: every event this service has ever consumed, from any of the 3
-- topics it subscribes to (payment-events, kitchen-events, delivery-events).
CREATE TABLE event_log (
  id BIGSERIAL PRIMARY KEY,
  topic VARCHAR(64) NOT NULL,
  partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,
  event_key VARCHAR(64),
  event_type VARCHAR(64) NOT NULL,
  order_id VARCHAR(40),
  payload JSONB NOT NULL,
  times_seen INT NOT NULL DEFAULT 1,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (topic, partition, kafka_offset)
);

CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_status_history_order_id ON order_status_history(order_id);
CREATE INDEX idx_event_log_order_id ON event_log(order_id);
