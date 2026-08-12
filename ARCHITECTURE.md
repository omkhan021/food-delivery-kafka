# Food Delivery Kafka Demo — Architecture & Contracts

This is the single source of truth every service must follow exactly so they interoperate.

## 1. Services & Ports

| Service              | Port | Role                                                                 |
|----------------------|------|-----------------------------------------------------------------------|
| order-service         | 8081 | Order CRUD, saga initiator/finalizer, status read-model, SSE, Kafka admin/replay API |
| payment-service        | 8082 | Dummy payment processing                                             |
| kitchen-service         | 8083 | Order prep simulation                                                |
| delivery-service        | 8084 | Delivery/driver simulation                                           |
| notification-service     | 8085 | Fan-in of all events, notification log, global SSE activity feed      |
| frontend (Vite/React)    | 5173 | SPA                                                                   |
| Kafka broker (docker)      | 9092 | KRaft single-node broker                                             |
| Kafka-UI (docker)          | 8080 | Topic/consumer-group browser                                         |
| Postgres (user's existing) | 5432 | Shared instance, database `fooddelivery`, one schema per service       |

All Spring Boot services: Java 17, Spring Boot 3.2.x, Maven (`pom.xml`, no wrapper — README states `mvn` prerequisite). All expose Spring Boot Actuator (`/actuator/health`) and enable CORS for `http://localhost:5173`. All configs are externalized in `src/main/resources/application.yml` with sane localhost defaults, overridable via env vars (`DB_USERNAME`, `DB_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`).

## 2. Database

Single Postgres database: `fooddelivery`. Each service owns one schema and manages it with **Flyway** (migrations in `src/main/resources/db/migration/V1__init.sql`, `spring.flyway.schemas=<schema>`, `spring.flyway.create-schemas=true`, `spring.jpa.hibernate.ddl-auto=validate` — Flyway is the only thing that creates/alters tables, Hibernate just validates against them).

Schemas: `order_service`, `payment_service`, `kitchen_service`, `delivery_service`, `notification_service`.

`scripts/init-db.sql` (written separately, run once by the user against their existing Postgres) creates the database and the 5 schemas plus a `fooddelivery` role.

## 3. Kafka Topics

4 topics, 3 partitions each, key = `orderId` (guarantees per-order ordering). Use `NewTopic` beans for auto-creation on first service startup (idempotent — `ifNotExists`).

- `order-events` — producer: order-service
- `payment-events` — producer: payment-service
- `kitchen-events` — producer: kitchen-service
- `delivery-events` — producer: delivery-service

Serialization: **plain JSON** via `JsonSerializer`/`JsonDeserializer` (Jackson), `spring.json.add.type.headers=false`, no schema registry. Each service defines its own copy of the DTO classes below (independent deployables — do not share a library module).

Consumer groups (must match exactly, used by the replay API):

| Consumer group id        | Service              | Subscribes to                                  |
|---------------------------|-----------------------|--------------------------------------------------|
| `payment-service-group`     | payment-service        | `order-events`                                    |
| `kitchen-service-group`     | kitchen-service         | `payment-events`                                  |
| `delivery-service-group`     | delivery-service        | `kitchen-events`                                  |
| `order-status-group`        | order-service           | `payment-events`, `kitchen-events`, `delivery-events` |
| `notification-service-group`  | notification-service      | `order-events`, `payment-events`, `kitchen-events`, `delivery-events` |

`@KafkaListener` `id` attribute must equal the consumer group id (the replay API looks up listener containers by this id via `KafkaListenerEndpointRegistry`).

## 4. Event JSON Contracts

All events share these 4 base fields, plus topic-specific optional fields. Use `@JsonInclude(JsonInclude.Include.NON_NULL)`. Timestamps are ISO-8601 `Instant` strings (UTC).

### 4.1 `order-events` → class `OrderEvent`
```json
{
  "eventId": "uuid-string",
  "eventType": "ORDER_PLACED | ORDER_COMPLETED | ORDER_CANCELLED",
  "orderId": "ORD-xxxxxxxx",
  "timestamp": "2026-08-12T14:30:00Z",
  "userId": "user-123",
  "paymentAmount": 42.50,
  "foodItems": [ {"itemName": "Margherita Pizza", "quantity": 2, "price": 12.99} ],
  "orderDate": "2026-08-12",
  "orderTime": "14:30:00",
  "reason": "payment failed after 3 attempts"
}
```
`userId`/`paymentAmount`/`foodItems`/`orderDate`/`orderTime` only populated on `ORDER_PLACED`. `reason` only on `ORDER_CANCELLED`. Nested type `FoodItem { itemName (String), quantity (int), price (BigDecimal) }`.

### 4.2 `payment-events` → class `PaymentEvent`
```json
{
  "eventId": "uuid", "eventType": "PAYMENT_PROCESSING | PAYMENT_COMPLETED | PAYMENT_FAILED",
  "orderId": "ORD-xxxxxxxx", "timestamp": "...",
  "userId": "user-123", "amount": 42.50,
  "transactionId": "TXN-xxxx",
  "failureReason": "card declined (simulated)"
}
```
`transactionId` only on `PAYMENT_COMPLETED`. `failureReason` only on `PAYMENT_FAILED`.

### 4.3 `kitchen-events` → class `KitchenEvent`
```json
{ "eventId":"uuid", "eventType":"ORDER_RECEIVED | PREPARING | PREPARED", "orderId":"...", "timestamp":"...", "estimatedMinutes": 15 }
```
`estimatedMinutes` only on `PREPARING`.

### 4.4 `delivery-events` → class `DeliveryEvent`
```json
{ "eventId":"uuid", "eventType":"DRIVER_ASSIGNED | PICKED_UP | ENROUTE | DELIVERED", "orderId":"...", "timestamp":"...", "driverId":"DRV-3", "driverName":"Sam Rivera", "etaMinutes": 12 }
```
`driverId`/`driverName` only on `DRIVER_ASSIGNED`. `etaMinutes` only on `ENROUTE`.

## 5. End-to-End Saga Flow

1. Client → `POST /api/orders` on order-service → row in `order_service.orders` (status `PLACED`) + `order_status_history` row → publish `ORDER_PLACED` to `order-events`.
2. payment-service consumes `ORDER_PLACED` → publish `PAYMENT_PROCESSING` immediately → after `payment.processing-delay-ms` (default 3000ms) → randomly (default 15% via `payment.failure-rate`) publish `PAYMENT_FAILED` (with reason) OR publish `PAYMENT_COMPLETED` (with generated `transactionId`). Persist to `payment_service.payments`.
3. order-service consumes payment-events:
   - `PAYMENT_COMPLETED` → order status → `PAYMENT_PROCESSING` done, next `RECEIVED_BY_KITCHEN` will come from kitchen-events.
   - `PAYMENT_FAILED` → order status → `CANCELLED`, publish `ORDER_CANCELLED` to `order-events` (terminal).
4. kitchen-service consumes `PAYMENT_COMPLETED` from `payment-events` → publish `ORDER_RECEIVED` → after `kitchen.prep-start-delay-ms` publish `PREPARING` (with `estimatedMinutes`) → after `kitchen.prep-duration-ms` publish `PREPARED`. Persist to `kitchen_service.kitchen_orders`.
5. delivery-service consumes `PREPARED` from `kitchen-events` → publish `DRIVER_ASSIGNED` (pick random dummy driver from a fixed in-memory list of ~6) → after `delivery.pickup-delay-ms` publish `PICKED_UP` → after `delivery.enroute-delay-ms` publish `ENROUTE` (with `etaMinutes`) → after `delivery.delivered-delay-ms` publish `DELIVERED`. Persist to `delivery_service.deliveries`.
6. order-service consumes delivery-events → updates status through `DRIVER_ASSIGNED`→`PICKED_UP`→`ENROUTE`→`DELIVERED`; on `DELIVERED`, after 2s locally, set status `COMPLETED` and publish `ORDER_COMPLETED` to `order-events` (closes the loop).
7. notification-service consumes all 4 topics the whole time, writes one row per event into `notification_service.notifications`, and pushes each event over its global SSE stream.

Order status enum (order-service, `orders.status`): `PLACED, PAYMENT_PROCESSING, PAYMENT_FAILED, CANCELLED, RECEIVED_BY_KITCHEN, PREPARING, PREPARED, DRIVER_ASSIGNED, PICKED_UP, ENROUTE, DELIVERED, COMPLETED`.

## 6. order-service Tables (Flyway `order_service` schema)

```sql
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

-- Audit / replay-visibility log: every event this service has ever consumed, from any of the 3 topics it subscribes to.
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
```
On consume, upsert into `event_log` with `ON CONFLICT (topic, partition, kafka_offset) DO UPDATE SET times_seen = event_log.times_seen + 1, last_seen_at = now()`. This is what makes replay visually provable: after a replay, `times_seen` jumps from 1 → 2 for every re-delivered offset, and the number is shown per-row in the frontend admin panel.

## 7. Other Services' Tables

**payment_service.payments**: `id BIGSERIAL PK, order_id, user_id, amount NUMERIC(10,2), status VARCHAR(16), transaction_id, failure_reason, created_at, updated_at`.

**kitchen_service.kitchen_orders**: `id BIGSERIAL PK, order_id, status VARCHAR(16), estimated_minutes INT, received_at, preparing_at, prepared_at, created_at, updated_at`.

**delivery_service.deliveries**: `id BIGSERIAL PK, order_id, driver_id, driver_name, status VARCHAR(16), eta_minutes INT, assigned_at, picked_up_at, enroute_at, delivered_at, created_at, updated_at`.

**notification_service.notifications**: `id BIGSERIAL PK, order_id, source_topic, event_type, message VARCHAR(256), created_at`.

## 8. REST APIs

### order-service (8081)
- `POST /api/orders` — body `{userId, foodItems:[{itemName,quantity,price}], orderDate?, orderTime?}` (paymentAmount computed server-side as sum of qty*price if not supplied; orderDate/orderTime default to now if omitted). Returns created order incl. `orderId`.
- `GET /api/orders` — list, newest first.
- `GET /api/orders/{orderId}` — order + items + full `order_status_history` timeline.
- `GET /api/orders/{orderId}/stream` — SSE (`text/event-stream`), emits an event named `status` with JSON `{orderId,status,note,timestamp}` every time a new history row is written for that order. Implement with a `ConcurrentHashMap<String, List<SseEmitter>>`.
- `GET /api/orders/{orderId}/event-log` — raw `event_log` rows for that order ordered by `first_seen_at`.
- `GET /api/admin/kafka/topics` — via `AdminClient`: topic name, partition count, per-partition end-offset.
- `GET /api/admin/kafka/consumer-groups` — via `AdminClient`: for each known group id (the 5 in section 3), state, and per-topic-partition lag (end offset − committed offset).
- `POST /api/admin/kafka/replay` — body `{"listenerId": "order-status-group"}` (must be one of the 5 group ids). Looks up the `MessageListenerContainer` (only possible for `order-status-group`, this service's own listener, via `KafkaListenerEndpointRegistry`) **or**, for the other 4 services' groups, uses `AdminClient.alterConsumerGroupOffsets` directly (those consumers must be temporarily down, or — simpler and what to implement — this endpoint always uses `AdminClient` to fetch the topic partitions for that group's subscribed topics, call `alterConsumerGroupOffsets(groupId, partitionsToOffsetAndMetadata(earliest))`; document clearly in a code comment + README that for a clean replay the target service should be briefly stopped first, OR restart that service afterward — that is normal, correct Kafka operational practice, not a bug). Response: which topic-partitions were reset and to what offset.

### payment-service (8082) / kitchen-service (8083) / delivery-service (8084)
- `GET /api/payments` | `/api/kitchen` | `/api/deliveries` — list all.
- `GET /api/payments/order/{orderId}` | `/api/kitchen/order/{orderId}` | `/api/deliveries/order/{orderId}` — by order.

### notification-service (8085)
- `GET /api/notifications` — list, newest first, capped at 200.
- `GET /api/notifications/order/{orderId}`.
- `GET /api/notifications/stream` — SSE, event name `notification`, JSON `{orderId, sourceTopic, eventType, message, timestamp}`, broadcast to ALL connected clients (global feed, not per-order).

## 9. Frontend (Vite + React, port 5173)

Env var `VITE_API_BASE_ORDER=http://localhost:8081`, `VITE_API_BASE_NOTIFICATION=http://localhost:8085` (others not called directly by UI, order-service is the aggregator).

Pages/panels in one SPA (simple client-side tab state, no router needed):
1. **Place Order** — form: user id (free text, default `user-101`), food item picker (fixed catalog of ~8 items with prices, qty steppers), computed total, submit → POST → navigate to tracking view for the new order.
2. **Track Order** — order id input + the most recent order auto-selected; horizontal stepper across `PLACED → PAYMENT_PROCESSING → RECEIVED_BY_KITCHEN → PREPARING → PREPARED → DRIVER_ASSIGNED → PICKED_UP → ENROUTE → DELIVERED → COMPLETED` (or a `CANCELLED` banner), fed by the SSE stream (`EventSource`) plus initial GET; full timeline list below the stepper with timestamps.
3. **All Orders** — table from `GET /api/orders`, click a row to open Track Order.
4. **Kafka Activity** — live console fed by notification-service SSE stream: scrolling list of `[topic] eventType — orderId — time`, auto-scroll, color-coded by topic.
5. **Admin / Replay** — calls order-service admin endpoints: topics table, consumer-group lag table, a dropdown of the 5 group ids + "Replay from beginning" button, and an event-log viewer (paste an order id) showing `times_seen` per row so a user can trigger replay and visibly watch `times_seen` increment.

Styling: plain CSS (no Tailwind needed, keep dependency footprint small) but should look clean/modern — card layout, decent spacing, a distinct color per lifecycle stage.

## 10. docker-compose.yml (repo root)

Services: `kafka` (KRaft single-node, e.g. `apache/kafka:3.7.0`, exposes `9092` on host, `PLAINTEXT` listener), `kafka-ui` (`provectuslabs/kafka-ui`, port `8080`, `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092`). No Postgres container (user's existing instance is reused). Include a commented-out optional `postgres` block for people who don't already have one.

## 11. Repo Layout

```
/
  ARCHITECTURE.md
  README.md
  docker-compose.yml
  scripts/init-db.sql
  order-service/          (pom.xml, src/main/java/com/fooddelivery/order/..., src/main/resources/application.yml, src/main/resources/db/migration/V1__init.sql)
  payment-service/         (same shape, package com.fooddelivery.payment)
  kitchen-service/          (package com.fooddelivery.kitchen)
  delivery-service/          (package com.fooddelivery.delivery)
  notification-service/       (package com.fooddelivery.notification)
  frontend/                (Vite React app)
```
