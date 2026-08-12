# kitchen-service

Simulates a restaurant kitchen reacting to completed payments, part of the
Food Delivery Kafka Demo (see `ARCHITECTURE.md` at the repo root for the full
cross-service contract). Port **8083**.

## What it does

1. Consumes `PAYMENT_COMPLETED` events from Kafka topic `payment-events`
   (consumer group `kitchen-service-group`). Other event types on that topic
   (`PAYMENT_PROCESSING`, `PAYMENT_FAILED`) are seen and explicitly ignored.
2. On `PAYMENT_COMPLETED`:
   - Persists a `kitchen_orders` row (schema `kitchen_service`, status `RECEIVED`).
   - Immediately publishes `ORDER_RECEIVED` to Kafka topic `kitchen-events` (key = `orderId`).
   - After `kitchen.prep-start-delay-ms` (default 2000ms), updates status to `PREPARING`
     and publishes `PREPARING` with a random `estimatedMinutes` between
     `kitchen.min-prep-minutes` and `kitchen.max-prep-minutes` (defaults 10-25).
   - After `kitchen.prep-duration-ms` (default 5000ms) from the PREPARING step,
     updates status to `PREPARED` and publishes `PREPARED`.
3. All delays run on a dedicated `ScheduledExecutorService` bean, never on the Kafka
   consumer thread - see the doc comments in `SchedulerConfig` and `KitchenOrderService`
   for a detailed explanation of why (and the trade-off: in-flight timers are in-memory
   only and don't survive a service restart - acceptable for this demo).

## REST API

| Method | Path                        | Description                          |
|--------|-----------------------------|---------------------------------------|
| GET    | `/api/kitchen`               | All kitchen orders, newest first       |
| GET    | `/api/kitchen/order/{orderId}` | Kitchen order for a single `orderId` (404 if not found) |
| GET    | `/actuator/health`           | Spring Boot Actuator health check      |

Example response shape:

```json
{
  "id": 1,
  "orderId": "ORD-abc12345",
  "status": "PREPARING",
  "estimatedMinutes": 18,
  "receivedAt": "2026-08-12T14:30:02Z",
  "preparingAt": "2026-08-12T14:30:04Z",
  "preparedAt": null,
  "createdAt": "2026-08-12T14:30:02Z",
  "updatedAt": "2026-08-12T14:30:04Z"
}
```

## Kafka contract

- **Consumes**: `payment-events` (group `kitchen-service-group`), filters for
  `eventType == "PAYMENT_COMPLETED"` only.
- **Produces**: `kitchen-events` (`NewTopic` bean, 3 partitions, replication factor 1,
  auto-created idempotently on startup), key = `orderId`.

`KitchenEvent` JSON shape (exact field names per ARCHITECTURE.md section 4.3):

```json
{ "eventId": "uuid", "eventType": "ORDER_RECEIVED | PREPARING | PREPARED", "orderId": "ORD-xxxxxxxx", "timestamp": "2026-08-12T14:30:00Z", "estimatedMinutes": 18 }
```

`estimatedMinutes` is only present on `PREPARING` events (`@JsonInclude(NON_NULL)` omits
it entirely elsewhere, it is never serialized as `null`).

Serialization is plain JSON (Jackson `JsonSerializer` / `JsonDeserializer`), no schema
registry, `spring.json.add.type.headers=false`. This service defines its own private
copies of `PaymentEvent` and `KitchenEvent` (not shared via a library) per the project's
independent-deployability rule.

## Database

- Schema: `kitchen_service` in the shared `fooddelivery` Postgres database.
- Table: `kitchen_orders` (`id`, `order_id` (unique), `status`, `estimated_minutes`,
  `received_at`, `preparing_at`, `prepared_at`, `created_at`, `updated_at`).
- Managed entirely by **Flyway** (`src/main/resources/db/migration/V1__init.sql`).
  Hibernate is configured with `ddl-auto: validate` - it never creates or alters
  tables, it only checks the JPA entity mapping matches what Flyway already built.
- The schema is auto-created by Flyway itself (`spring.flyway.create-schemas=true`),
  but the shared database `fooddelivery` and the `fooddelivery` role must already
  exist (see `scripts/init-db.sql` at the repo root, run once by hand).

## Configuration / environment variables

All settings live in `src/main/resources/application.yml` with localhost-friendly
defaults, overridable via environment variables:

| Property                        | Env var / default                                   |
|----------------------------------|--------------------------------------------------------|
| `server.port`                    | `8083`                                                  |
| `spring.datasource.url`          | `jdbc:postgresql://localhost:5432/fooddelivery`         |
| `spring.datasource.username`     | `DB_USERNAME` (default `postgres`)                       |
| `spring.datasource.password`     | `DB_PASSWORD` (default `postgres`)                       |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)      |
| `kitchen.prep-start-delay-ms`    | default `2000` (ms before RECEIVED -> PREPARING)          |
| `kitchen.prep-duration-ms`       | default `5000` (ms before PREPARING -> PREPARED)          |
| `kitchen.min-prep-minutes`       | default `10` (lower bound of the random `estimatedMinutes`) |
| `kitchen.max-prep-minutes`       | default `25` (upper bound of the random `estimatedMinutes`) |
| `app.cors.allowed-origin`        | default `http://localhost:5173`                          |

The `kitchen.*` delay values are intentionally small (seconds) so a demo saga
completes quickly - they are not meant to represent real prep-time minutes; only
the *value* published as `estimatedMinutes` is meant to look realistic (10-25).

## Running standalone

Prerequisites: Java 17, Maven (`mvn`), a running Postgres with the `fooddelivery`
database and `kitchen_service` schema-owning role (see repo-root `scripts/init-db.sql`),
and a running Kafka broker (see repo-root `docker-compose.yml` for the KRaft broker +
Kafka-UI). kitchen-service only needs `payment-events` messages to actually do
anything end-to-end, but it starts up fine on its own for local iteration.

```bash
cd kitchen-service

# uses localhost:5432/fooddelivery, postgres/postgres, and localhost:9092 by default
mvn spring-boot:run

# or, overriding env vars:
DB_USERNAME=postgres DB_PASSWORD=postgres KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  mvn spring-boot:run

# build + run the jar
mvn clean package
java -jar target/kitchen-service-1.0.0.jar
```

Health check: `curl http://localhost:8083/actuator/health`

### Trying it manually without the other services

You can drive kitchen-service on its own by publishing a `PAYMENT_COMPLETED` message
directly to `payment-events` (e.g. via Kafka-UI's "Produce Message" screen, or
`kafka-console-producer.sh --topic payment-events --property "parse.key=true"
--property "key.separator=:"`), with key = an orderId string and value e.g.:

```json
{"eventId":"11111111-1111-1111-1111-111111111111","eventType":"PAYMENT_COMPLETED","orderId":"ORD-demo0001","timestamp":"2026-08-12T14:30:00Z","userId":"user-101","amount":24.50,"transactionId":"TXN-demo"}
```

Then watch the logs and `GET /api/kitchen/order/ORD-demo0001` as it moves through
RECEIVED -> PREPARING -> PREPARED, and/or watch `kitchen-events` in Kafka-UI.
