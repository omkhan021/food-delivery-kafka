# delivery-service

Delivery/driver simulation microservice for the Food Delivery Kafka Demo. Port **8084**.

Part of a larger multi-service Kafka demo (order-service, payment-service, kitchen-service,
delivery-service, notification-service, frontend). See `../ARCHITECTURE.md` at the repo root for
the full system contract. This README only covers running this service standalone.

## What it does

1. Consumes `PREPARED` events from the `kitchen-events` topic (consumer group
   `delivery-service-group`). `ORDER_RECEIVED` and `PREPARING` events on the same topic are
   ignored.
2. On `PREPARED`, persists a row in `delivery_service.deliveries`, picks a random driver from a
   fixed in-memory roster of 6 dummy drivers, sets status `ASSIGNED`, and publishes
   `DRIVER_ASSIGNED` to `delivery-events` (key = `orderId`).
3. Runs a **non-blocking, 3-stage delayed state machine** on a shared `ScheduledExecutorService`
   (see `SchedulerConfig` / `DeliveryService`) that progresses the delivery through:
   - `PICKED_UP` (after `delivery.pickup-delay-ms`)
   - `ENROUTE` (after `delivery.enroute-delay-ms`, with a randomly generated `etaMinutes`)
   - `DELIVERED` (after `delivery.delivered-delay-ms`)

   Each stage publishes the corresponding event to `delivery-events`. The delays are demo-friendly
   (a few seconds) so the whole saga is visible quickly in the UI, but the `etaMinutes` value
   still looks like a realistic delivery estimate.

## Driver roster (fixed, in-memory — `DriverRoster.java`)

| Driver ID | Name         |
|-----------|--------------|
| DRV-1     | Sam Rivera   |
| DRV-2     | Jordan Lee   |
| DRV-3     | Priya Nair   |
| DRV-4     | Marcus Webb  |
| DRV-5     | Ana Torres   |
| DRV-6     | Kenji Sato   |

## REST API

| Method | Path                            | Description                          |
|--------|----------------------------------|---------------------------------------|
| GET    | `/api/deliveries`                | List all deliveries, newest first     |
| GET    | `/api/deliveries/order/{orderId}` | Deliveries for a given order (list — see note below) |
| GET    | `/actuator/health`               | Health check                          |

> **Note:** `/api/deliveries/order/{orderId}` returns a JSON array rather than a single object.
> In normal operation there's exactly one delivery row per order, but a Kafka topic replay could
> in principle redeliver a `PREPARED` event and create a second row for the same order — returning
> a list makes that visible instead of silently picking one.

## Running standalone

### Prerequisites
- Java 17
- Maven (`mvn`) — no wrapper included, per repo convention
- A running Postgres instance with a `fooddelivery` database and a `delivery_service` schema
  (see `../scripts/init-db.sql` at the repo root, or create manually — Flyway will create tables
  inside the schema on first boot as long as the schema itself exists or
  `spring.flyway.create-schemas=true` is honored)
- A running Kafka broker reachable at `localhost:9092` (see `../docker-compose.yml` at the repo
  root for a one-command KRaft broker + Kafka-UI)

### Build & run

```bash
cd delivery-service
mvn spring-boot:run
```

or build a jar and run it:

```bash
mvn clean package
java -jar target/delivery-service-1.0.0.jar
```

The service starts on `http://localhost:8084`. On first boot, Flyway runs
`src/main/resources/db/migration/V1__init.sql` against the `delivery_service` schema, and a
`NewTopic` bean creates the `delivery-events` topic (3 partitions, replication factor 1) if it
doesn't already exist.

### Verifying it works end-to-end

This service only *reacts* to `PREPARED` events on `kitchen-events` — it can't do anything useful
in isolation. To see it work, either:
- Run the full stack (`order-service`, `payment-service`, `kitchen-service`, this service, Kafka)
  and place an order through `order-service`'s `POST /api/orders`, or
- Manually produce a `PREPARED` message to `kitchen-events` with a Kafka console producer /
  Kafka-UI, e.g.:
  ```json
  { "eventId": "test-1", "eventType": "PREPARED", "orderId": "ORD-TEST01", "timestamp": "2026-08-12T14:30:00Z" }
  ```
  Then watch `GET /api/deliveries/order/ORD-TEST01` and the application logs as the delivery
  progresses through ASSIGNED -> PICKED_UP -> ENROUTE -> DELIVERED, and check `delivery-events`
  in Kafka-UI for the published events.

## Configuration / environment variables

All configuration lives in `src/main/resources/application.yml` with sane localhost defaults,
overridable via environment variables:

| Property                        | Env var                  | Default                                      | Notes |
|----------------------------------|---------------------------|-----------------------------------------------|-------|
| `spring.datasource.url`          | —                          | `jdbc:postgresql://localhost:5432/fooddelivery` | Shared DB, `delivery_service` schema |
| `spring.datasource.username`     | `DB_USERNAME`              | `postgres`                                    |       |
| `spring.datasource.password`     | `DB_PASSWORD`              | `postgres`                                    |       |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS`  | `localhost:9092`                              |       |
| `delivery.pickup-delay-ms`       | —                          | `3000`                                        | ASSIGNED -> PICKED_UP delay |
| `delivery.enroute-delay-ms`      | —                          | `3000`                                        | PICKED_UP -> ENROUTE delay |
| `delivery.delivered-delay-ms`    | —                          | `5000`                                        | ENROUTE -> DELIVERED delay |
| `delivery.min-eta-minutes`       | —                          | `15`                                          | Lower bound for random `etaMinutes` |
| `delivery.max-eta-minutes`       | —                          | `35`                                          | Upper bound for random `etaMinutes` (inclusive) |
| `cors.allowed-origin`            | —                          | `http://localhost:5173`                       | Frontend origin allowed via CORS |
| `server.port`                    | —                          | `8084`                                        |       |

The `delivery.*` properties are plain `spring.*`-style externalized config (no env var mapping
defined for them since they're demo-tuning knobs, not deployment secrets) — override them with
`-Ddelivery.pickup-delay-ms=1000` or a `SPRING_APPLICATION_JSON` / profile-specific
`application.yml` if you want a snappier or slower demo.

## Kafka contracts

- **Consumes:** `kitchen-events` (group `delivery-service-group`), reacts only to `eventType=PREPARED`.
- **Produces:** `delivery-events` (3 partitions, replication 1, auto-created via `NewTopic` bean),
  key = `orderId`, event types `DRIVER_ASSIGNED | PICKED_UP | ENROUTE | DELIVERED`.

`DeliveryEvent` JSON shape (matches ARCHITECTURE.md section 4.4 exactly):

```json
{ "eventId": "uuid", "eventType": "DRIVER_ASSIGNED | PICKED_UP | ENROUTE | DELIVERED", "orderId": "ORD-xxxxxxxx", "timestamp": "2026-08-12T14:30:00Z", "driverId": "DRV-3", "driverName": "Priya Nair", "etaMinutes": 27 }
```

`driverId` / `driverName` only present on `DRIVER_ASSIGNED`. `etaMinutes` only present on
`ENROUTE`. Serialization is plain JSON via Jackson (`spring.json.add.type.headers=false`, no
schema registry) — this service keeps its own private copy of the `KitchenEvent` and
`DeliveryEvent` DTO classes rather than sharing a library module with sibling services, per the
architecture's "independent deployables" rule.

## What other services / the frontend need to know

- **order-service** consumes `delivery-events` (as part of `order-status-group`, alongside
  `payment-events` and `kitchen-events`) to drive the order status timeline through
  `DRIVER_ASSIGNED -> PICKED_UP -> ENROUTE -> DELIVERED`.
- **notification-service** consumes `delivery-events` (as part of `notification-service-group`)
  to log/broadcast every delivery event.
- The frontend never calls delivery-service directly — order-service is the aggregator (see
  ARCHITECTURE.md section 9). `GET /api/deliveries` and `GET /api/deliveries/order/{orderId}` on
  this service exist mainly for debugging/inspection and the Kafka-UI/admin story.
- `delivery-events` events for `PICKED_UP` and `DELIVERED` do **not** include `driverId`/
  `driverName`/`etaMinutes` (per the `NON_NULL` inclusion rule) — consumers should already have
  `driverId`/`driverName` cached from the `DRIVER_ASSIGNED` event if they need it alongside later
  stages.
