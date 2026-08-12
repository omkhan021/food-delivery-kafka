# notification-service

Fan-in notification / activity-feed microservice for the Food Delivery Kafka demo (see
`../ARCHITECTURE.md` for the full cross-service contract). This service:

- Subscribes to **all 4** domain event topics — `order-events`, `payment-events`,
  `kitchen-events`, `delivery-events` — under **one** consumer group,
  `notification-service-group`.
- Persists one row per consumed event into `notification_service.notifications`.
- Broadcasts each event to every currently-connected client on a global Server-Sent
  Events feed, `GET /api/notifications/stream`.
- **Never produces to Kafka.** It's a pure consumer/sink — no `NewTopic` beans are
  declared here; the 4 topics are owned and created by their respective producer
  services (order-service, payment-service, kitchen-service, delivery-service).

## Why one consumer group across 4 topics is safe

A Kafka consumer group is just a named set of consumers sharing work; nothing requires
a group to be scoped to a single topic. `@KafkaListener` can list several topic names,
and the group gets independent partition assignment and committed offsets *per topic*.
notification-service doesn't need cross-topic ordering (it only needs to see and log
every event, once, in roughly received order), so one listener/group reading all 4
topics is simpler than 4 separate containers. See the Javadoc on
`config/KafkaConsumerConfig` and `kafka/NotificationEventListener` for more detail.

## How the SSE broadcast works

`service/SseBroadcaster` keeps a `CopyOnWriteArrayList<SseEmitter>` of every client
currently connected to `/api/notifications/stream`. Every time the Kafka listener
processes an event, `NotificationService` builds a message, saves it, and calls
`broadcaster.broadcast(...)`, which iterates the emitter list and pushes an SSE event
named `notification` to each one. Emitters are removed from the list on completion,
timeout, or send failure so the list never accumulates dead connections. This is a
**global broadcast** (every client sees every event from every topic) — unlike
order-service's per-order tracking stream, there is no per-order filtering here; that's
what makes it useful as a live "Kafka Activity" console in the frontend.

## Tech stack

Java 17, Spring Boot 3.2.5, Maven, spring-kafka, Spring Data JPA + Flyway (Postgres),
Spring Boot Actuator.

## Running standalone

Prerequisites: Java 17, Maven, a running Kafka broker reachable at
`KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`), and a Postgres instance with a
`fooddelivery` database and a `notification_service` schema/role reachable at the
configured JDBC URL (see `scripts/init-db.sql` at the repo root, or just let
`spring.flyway.create-schemas=true` create the schema on first run if your DB user has
permission).

From this directory:

```bash
mvn spring-boot:run
```

or build and run the jar:

```bash
mvn clean package
java -jar target/notification-service-1.0.0.jar
```

The service listens on **port 8085**. Health check: `GET http://localhost:8085/actuator/health`.

You can run this service in isolation even before the other 4 services exist — it will
sit idle waiting for messages (and auto-create the 4 topics as an empty consumer-group
side effect, if the broker allows auto-creation) and start emitting notifications as
soon as any producer service starts publishing.

## Configuration / environment variables

All configuration lives in `src/main/resources/application.yml` with localhost-friendly
defaults, overridable via env vars:

| Variable                  | Default                                    | Purpose                          |
|----------------------------|---------------------------------------------|-----------------------------------|
| `DB_USERNAME`               | `postgres`                                   | Postgres username                 |
| `DB_PASSWORD`               | `postgres`                                   | Postgres password                 |
| `KAFKA_BOOTSTRAP_SERVERS`     | `localhost:9092`                             | Kafka broker(s)                    |

Fixed (not env-overridable) config properties of note:

| Property                          | Value                        | Purpose                                              |
|-------------------------------------|--------------------------------|---------------------------------------------------------|
| `server.port`                        | `8085`                         | HTTP port                                                |
| `spring.datasource.url`               | `jdbc:postgresql://localhost:5432/fooddelivery` | Fixed per ARCHITECTURE.md (shared DB, one schema each) |
| `spring.flyway.schemas` / `spring.jpa...default_schema` | `notification_service` | This service's owned schema                      |
| `spring.jpa.hibernate.ddl-auto`        | `validate`                     | Flyway owns DDL; Hibernate only validates                |
| `notification.max-list-size`           | `200`                          | Cap on `GET /api/notifications` rows returned             |

## REST API

- `GET /api/notifications` — list, newest first, capped at `notification.max-list-size` (200).
- `GET /api/notifications/order/{orderId}` — all notifications for one order, newest first.
- `GET /api/notifications/stream` — SSE (`text/event-stream`), event name `notification`,
  payload `{orderId, sourceTopic, eventType, message, timestamp}`. Global feed — every
  connected client receives every event.
- `GET /actuator/health` — Spring Boot Actuator health.

CORS is enabled for `http://localhost:5173` (the Vite frontend) across all endpoints,
including the SSE stream.

## Database

Schema `notification_service`, single table `notifications`
(`id, order_id, source_topic, event_type, message, created_at`), created by the Flyway
migration `src/main/resources/db/migration/V1__init.sql`. Hibernate's `ddl-auto` is
`validate` — never change the schema by editing the JPA entity; add a new
`V2__...sql` migration instead.

## Error handling notes

- Kafka deserialization failures (malformed JSON on any of the 4 topics) are caught by
  an `ErrorHandlingDeserializer` + the container's `DefaultErrorHandler`
  (`config/KafkaConsumerConfig`): the bad record is logged and skipped after 2 quick
  retries, the consumer thread keeps running.
- Any other unexpected failure while building a message or persisting a row is caught
  in `kafka/NotificationEventListener#onEvent` and logged — it never propagates and
  kills the listener.
- A message missing `eventType` is still recorded (as `eventType=UNKNOWN`) rather than
  silently dropped, and a human-readable fallback message is generated for any
  `eventType` this service doesn't explicitly recognize, so new event types added by
  other services degrade gracefully instead of breaking this service.
