# order-service

Port **8081**. Saga initiator and finalizer for the Food Delivery Kafka Demo (see the root
`ARCHITECTURE.md` for the full cross-service spec).

- Accepts `POST /api/orders`, persists the order (Postgres, Flyway-managed `order_service`
  schema), and publishes `ORDER_PLACED` to `order-events`.
- Consumes `payment-events`, `kitchen-events`, `delivery-events` (single `@KafkaListener`,
  `id`/`groupId` = `order-status-group`) to maintain the order status read-model, with
  idempotent/replay-safe handling backed by the `event_log` audit table.
- Closes the saga: `PAYMENT_FAILED` → `ORDER_CANCELLED`; `DELIVERED` → (2s later) `COMPLETED` →
  `ORDER_COMPLETED`.
- Exposes SSE (`/api/orders/{orderId}/stream`) and Kafka admin/replay endpoints
  (`/api/admin/kafka/...`).

## Prerequisites

- Java 17
- Maven (`mvn`) — no wrapper is included, per project convention
- A running Kafka broker (see root `docker-compose.yml`) reachable at `localhost:9092`
- A Postgres instance with database `fooddelivery` and schema `order_service` reachable at
  `localhost:5432` (run `scripts/init-db.sql` once against your existing Postgres, per the root
  README)

## Running standalone

```bash
cd order-service
mvn spring-boot:run
```

The service starts on `http://localhost:8081`. Flyway runs `V1__init.sql` automatically on
startup (creating the `order_service` schema/tables if they don't exist), and the 4 Kafka topics
(`order-events`, `payment-events`, `kitchen-events`, `delivery-events`) are auto-created
(3 partitions, replication factor 1) via `NewTopic` beans if they don't already exist.

Health check: `GET http://localhost:8081/actuator/health`.

## Environment variables

| Variable                 | Default                        | Purpose                          |
|---------------------------|----------------------------------|-------------------------------------|
| `DB_USERNAME`               | `postgres`                        | Postgres username                     |
| `DB_PASSWORD`               | `postgres`                        | Postgres password                     |
| `KAFKA_BOOTSTRAP_SERVERS`      | `localhost:9092`                    | Kafka broker address(es)                |

`spring.datasource.url` is hardcoded to `jdbc:postgresql://localhost:5432/fooddelivery` (per the
shared spec, all services point at the same local Postgres instance / database, one schema each).

## REST API

| Method | Path                                     | Notes                                                        |
|--------|--------------------------------------------|------------------------------------------------------------------|
| POST   | `/api/orders`                                | Body `{userId, foodItems:[{itemName,quantity,price}], orderDate?, orderTime?, paymentAmount?}`. `paymentAmount` is computed server-side as `sum(quantity*price)` when omitted. |
| GET    | `/api/orders`                                | List, newest first.                                             |
| GET    | `/api/orders/{orderId}`                        | Order + items + full status history.                            |
| GET    | `/api/orders/{orderId}/stream`                   | SSE (`text/event-stream`), event name `status`.                    |
| GET    | `/api/orders/{orderId}/event-log`                  | Raw `event_log` rows for the order, ordered by `first_seen_at`.        |
| GET    | `/api/admin/kafka/topics`                       | Per-topic partition count + end offsets.                          |
| GET    | `/api/admin/kafka/consumer-groups`                  | Per-group state + per-partition lag for all 5 known consumer groups.    |
| POST   | `/api/admin/kafka/replay`                       | Body `{"listenerId": "<one of the 5 group ids>"}`. Resets that group's committed offsets to earliest. |

### Replay operational notes

`POST /api/admin/kafka/replay` always resets offsets via `AdminClient.alterConsumerGroupOffsets`.

- For **`order-status-group`** (this service's own listener), the endpoint pauses the live
  `MessageListenerContainer` via `KafkaListenerEndpointRegistry` before resetting offsets and
  resumes it immediately after — the replay is then processed live by this same running process.
- For the other 4 groups (`payment-service-group`, `kitchen-service-group`,
  `delivery-service-group`, `notification-service-group`), this service has no control over
  those processes. Kafka's `alterConsumerGroupOffsets` only succeeds when the target group has no
  active members, so for a clean replay either briefly stop the owning service before calling
  this endpoint, or call it and then restart that service so it picks up the reset offset on its
  next poll. This is normal, correct Kafka operational practice, not a bug.

Watch `times_seen` increment on `GET /api/orders/{orderId}/event-log` after a replay — that's the
visible proof the same Kafka offset was reprocessed, and the "genuinely new vs. replay" logic in
`KafkaEventConsumerService` is what keeps `order_status_history` and SSE pushes free of
duplicates despite the redelivery.

## Design notes / implementation choices

- **Consumer value deserialization**: `spring.kafka.consumer.value-deserializer` is
  `StringDeserializer`. The single `order-status-group` listener parses each record's JSON body
  manually with Jackson's `ObjectMapper`/`JsonNode` and dispatches on `topic` + `eventType`,
  rather than using a typed `JsonDeserializer` per topic. This lets one `@KafkaListener` (as
  required by the spec, since the replay API looks up listener containers by id) cleanly handle
  the 3 different event shapes on `payment-events`/`kitchen-events`/`delivery-events`. See the
  javadoc on `KafkaEventConsumerService` and the comment in `application.yml` for the full
  rationale.
- **Commit strategy**: `enable-auto-commit=false` with an explicit
  `ConcurrentKafkaListenerContainerFactory` using `AckMode.RECORD` — each record's offset is only
  committed after the listener method returns normally. A processing exception is caught and
  logged (not rethrown) so one bad message can't wedge the whole listener, but see
  `event_log`/`times_seen` for how idempotency is handled independent of commit timing.
- **Idempotent consumption**: every consumed record is first upserted into `event_log` keyed by
  `(topic, partition, kafka_offset)`. Only when the upsert reports `times_seen == 1` (first-ever
  delivery of that exact offset) does the service mutate `orders.status`, append an
  `order_status_history` row, or push an SSE update — replays/redeliveries still bump
  `times_seen`/`last_seen_at` but are otherwise no-ops for the read-model. This is the standard
  idempotent-consumer pattern for building safe read-models on top of Kafka's at-least-once
  delivery.
- **`PAYMENT_COMPLETED` status mapping**: there's no dedicated `orders.status` enum value for
  "payment completed" (the next visible status is `RECEIVED_BY_KITCHEN`, driven by
  `kitchen-events`). Consuming `PAYMENT_COMPLETED` still appends a status-history note (e.g.
  "Payment completed (transactionId=...)") for traceability, but doesn't change `orders.status`.
  This is an implementation interpretation of ARCHITECTURE.md section 5.3 ("order status ->
  PAYMENT_PROCESSING done, next RECEIVED_BY_KITCHEN will come from kitchen-events"), not a
  deviation from any named field/topic/status value.
- **DLQ**: no separate dead-letter topic is implemented (out of scope for this demo). Poison
  messages are caught, logged with a `[DLQ-LOG]` marker (see `KafkaEventConsumerService`), and
  skipped so the consumer group keeps progressing.

## Deviations from ARCHITECTURE.md

None to the contracts (topic names, event JSON shapes, table DDL, ports, REST paths all match the
spec exactly). Two small additive, non-breaking extras:
- Added `idx_order_items_order_id`, `idx_order_status_history_order_id`, `idx_event_log_order_id`
  indexes in `V1__init.sql`, on top of the DDL specified in section 6 (pure performance aid, no
  schema/column change).
- `POST /api/orders` accepts an optional top-level `paymentAmount` field in addition to the
  documented `foodItems` (the spec says paymentAmount is "computed server-side ... if not
  supplied", implying a client may supply it explicitly).

## What other services / the frontend need to know

- This is the **only** producer of `order-events`; consume it if you need `ORDER_PLACED` /
  `ORDER_CANCELLED` / `ORDER_COMPLETED`.
- The frontend (`VITE_API_BASE_ORDER=http://localhost:8081`) is the aggregator for order data —
  it should call this service for order CRUD, the per-order SSE stream, and the Kafka admin/
  replay panel. It should **not** need to call payment/kitchen/delivery services directly.
- CORS is open for `http://localhost:5173` only.
