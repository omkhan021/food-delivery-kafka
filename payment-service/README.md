# payment-service

Part of the Food Delivery Kafka Demo (see `../ARCHITECTURE.md` for the full cross-service spec).

**This is a DUMMY payment gateway.** It does not integrate with any real payment processor
(Stripe, etc.). It only simulates one: a fixed/configurable delay followed by a randomly chosen
success or failure outcome. The whole point is to demonstrate an async, event-driven "processing
now / result later" pattern over Kafka without needing real payment credentials.

## What it does

1. Listens to the `order-events` topic (consumer group `payment-service-group`) for
   `ORDER_PLACED` events. Other event types on that topic (`ORDER_COMPLETED`, `ORDER_CANCELLED`)
   are ignored.
2. On `ORDER_PLACED`:
   - Inserts a row into `payment_service.payments` with status `PENDING`.
   - Publishes `PAYMENT_PROCESSING` to `payment-events` immediately (key = `orderId`).
   - Schedules a one-shot delayed task (on a dedicated `ScheduledExecutorService`, **not** the
     Kafka consumer thread - see the comment in `SchedulerConfig` for why that matters) that,
     after `payment.processing-delay-ms`:
     - Randomly fails with probability `payment.failure-rate`. On failure: updates the row to
       `FAILED` with a rotating dummy `failureReason` and publishes `PAYMENT_FAILED`.
     - Otherwise succeeds: updates the row to `COMPLETED` with a generated
       `transactionId` (`"TXN-" + UUID`) and publishes `PAYMENT_COMPLETED`.
3. Exposes a read-only REST API over the `payments` table.

## Run standalone

Prerequisites: Java 17, Maven, a running Postgres with the `fooddelivery` database and
`payment_service` schema (see `../scripts/init-db.sql`), and a running Kafka broker on
`localhost:9092` (see `../docker-compose.yml`).

```bash
cd payment-service
mvn spring-boot:run
```

The service starts on **port 8082**. Flyway runs its migration (`V1__init.sql`) automatically on
startup and creates the `payments` table if it doesn't already exist. The `payment-events` topic
is created automatically (3 partitions, replication factor 1) if it doesn't already exist.

To see it do something, run order-service and POST an order to it (`POST /api/orders` on
`localhost:8081`) - that publishes `ORDER_PLACED`, which this service will pick up within a few
seconds (assuming the consumer group is caught up).

## Configuration

All config lives in `src/main/resources/application.yml` with localhost-friendly defaults,
overridable via environment variables:

| Property | Env var | Default | Notes |
|---|---|---|---|
| `server.port` | - | `8082` | fixed by ARCHITECTURE.md |
| `spring.datasource.url` | - | `jdbc:postgresql://localhost:5432/fooddelivery` | |
| `spring.datasource.username` | `DB_USERNAME` | `postgres` | |
| `spring.datasource.password` | `DB_PASSWORD` | `postgres` | |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `payment.processing-delay-ms` | - | `3000` | ms between `PAYMENT_PROCESSING` and the final outcome |
| `payment.failure-rate` | - | `0.15` | probability (0.0-1.0) a simulated payment fails |
| `cors.allowed-origin` | - | `http://localhost:5173` | frontend origin allowed via CORS |

`payment.processing-delay-ms` and `payment.failure-rate` aren't currently wired to env vars (no
env var name was specified in ARCHITECTURE.md for them) - edit `application.yml` directly, or add
`-Dpayment.processing-delay-ms=...` / `-Dpayment.failure-rate=...` on the command line, or a
`PAYMENT_PROCESSING_DELAY_MS` / `PAYMENT_FAILURE_RATE` env var also works out of the box since
Spring Boot's relaxed binding maps `payment.processing-delay-ms` to that env var name
automatically.

## Database

Schema `payment_service`, table `payments`, Flyway-managed
(`src/main/resources/db/migration/V1__init.sql`). Hibernate is configured with
`ddl-auto=validate` - it never creates or alters tables itself, only validates the JPA mapping
against what Flyway already created.

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL PK` | |
| `order_id` | `VARCHAR(40)` | matches `orders.order_id` from order-service |
| `user_id` | `VARCHAR(64)` | |
| `amount` | `NUMERIC(10,2)` | copied from the order's `paymentAmount` |
| `status` | `VARCHAR(16)` | `PENDING` \| `COMPLETED` \| `FAILED` |
| `transaction_id` | `VARCHAR(64)` | set only when `COMPLETED` |
| `failure_reason` | `VARCHAR(256)` | set only when `FAILED` |
| `created_at` / `updated_at` | `TIMESTAMPTZ` | |

## Kafka

- Consumes: `order-events` (consumer group `payment-service-group`), filters to `ORDER_PLACED`.
- Produces: `payment-events` (`PAYMENT_PROCESSING`, `PAYMENT_COMPLETED`, `PAYMENT_FAILED`), key =
  `orderId`, plain JSON via Spring Kafka's `JsonSerializer` (no schema registry, no type headers -
  `spring.json.add.type.headers=false`).
- The consumer's value deserializer is wrapped in Spring Kafka's `ErrorHandlingDeserializer`
  around the real `JsonDeserializer`, paired with a `DefaultErrorHandler` bean
  (`KafkaConsumerConfig`) that logs and skips any message that fails to deserialize, instead of
  crashing the listener container. Errors raised inside the listener method itself are also
  caught and logged there (`OrderEventListener`) for the same reason - one bad/unexpected message
  must never take down the consumer.

## REST API

- `GET /api/payments` - all payments, newest first.
- `GET /api/payments/order/{orderId}` - the payment record for one order (404 if none exists
  yet - e.g. the `ORDER_PLACED` event hasn't been consumed yet, or the order id doesn't exist).
- `GET /actuator/health` - Spring Boot Actuator health check.

## Notes for other services / frontend

- payment-service never talks to order-service, kitchen-service, delivery-service, or the
  frontend directly - it only communicates via the `order-events` (consume) and `payment-events`
  (produce) Kafka topics, plus its own tiny read API above (which nothing else in the system
  currently calls - order-service is the aggregator the frontend talks to).
- If you send a hand-crafted `ORDER_PLACED` event for testing, make sure `orderId`, `userId`, and
  `paymentAmount` are populated - those are the three fields this service reads from it.
- The 15% default failure rate is intentional so the "payment failed -> order cancelled" path in
  the saga is easy to observe during a demo without needing to force it.
