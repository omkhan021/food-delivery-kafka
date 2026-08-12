# Food Delivery Kafka Demo

A real, runnable Uber-Eats-style food ordering & delivery system built specifically to show **Kafka event streaming end-to-end** — five independent Spring Boot microservices talking only through Kafka, a React dashboard to watch it happen live, and a working **replay** demo you can trigger from the UI.

Full technical spec (topics, event JSON shapes, DB schemas, REST APIs): see [`ARCHITECTURE.md`](./ARCHITECTURE.md).

## What it demonstrates

- **Event-driven saga across services** — placing an order kicks off a chain of Kafka events across payment → kitchen → delivery, with no service calling another directly.
- **Full order lifecycle**: `PLACED → PAYMENT_PROCESSING → RECEIVED_BY_KITCHEN → PREPARING → PREPARED → DRIVER_ASSIGNED → PICKED_UP → ENROUTE → DELIVERED → COMPLETED` (plus a `CANCELLED` branch on simulated payment failure).
- **Consumer groups & fan-out** — one topic (e.g. `payment-events`) is independently consumed by two different services (order-service and kitchen-service) at their own pace.
- **Idempotent consumption & replay** — order-service logs every Kafka record it has ever seen (topic/partition/offset) with a `times_seen` counter. Reset a consumer group's offsets to the beginning from the Admin tab and watch `times_seen` jump from 1→2 for every event, while the order's actual status stays correct (no duplicate side effects) — a concrete, visible demonstration of exactly-once *processing* on top of at-least-once *delivery*.
- **A non-idempotent contrast** — notification-service does *not* dedupe, so replaying it will visibly re-emit duplicate notifications for the same events. Comparing the two is a good way to *feel* why idempotent consumers matter.

## Architecture at a glance

```
                         ┌─────────────────┐
   POST /api/orders ───▶ │  order-service   │──▶ order-events (ORDER_PLACED)
                         │  (8081)          │
                         └────────▲─────────┘
                                  │ status read-model + SSE + Kafka admin/replay API
                                  │ (consumes payment/kitchen/delivery-events)
        order-events              │
        payment-events            │
        kitchen-events            │
        delivery-events           │
                                  │
   order-events ──▶ payment-service (8082) ──▶ payment-events (PAYMENT_PROCESSING/COMPLETED/FAILED)
   payment-events ──▶ kitchen-service (8083) ──▶ kitchen-events (ORDER_RECEIVED/PREPARING/PREPARED)
   kitchen-events ──▶ delivery-service (8084) ──▶ delivery-events (DRIVER_ASSIGNED/PICKED_UP/ENROUTE/DELIVERED)

   all 4 topics ──▶ notification-service (8085) ──▶ notifications table + global SSE feed

   React frontend (5173) talks to order-service (orders + admin/replay) and
   notification-service (live Kafka activity feed) over REST + SSE.
```

Kafka topics: `order-events`, `payment-events`, `kitchen-events`, `delivery-events` — plain JSON payloads, no schema registry, keyed by `orderId` (per-order ordering guaranteed).

## Prerequisites

- **Java 17+** and **Maven 3.9+**
- **Node 18+** and npm
- **Docker Desktop** (for Kafka — you said you already have this)
- **PostgreSQL running locally** (you said this is already installed/running), reachable at `localhost:5432` with a superuser (defaults assume `postgres`/`admin123` — override via `DB_USERNAME`/`DB_PASSWORD` env vars on each service if yours differ)

## Run it

### 1. Provision the database

```bash
psql -h localhost -U postgres -f scripts/init-db.sql
```

This creates the `fooddelivery` database and 5 schemas (one per service). Each service then creates its own tables inside its schema automatically on first startup via **Flyway** — you never hand-write `CREATE TABLE`.

### 2. Start Kafka

```bash
docker compose up -d
```

This starts a single-node Kafka broker (KRaft mode, no Zookeeper) on `localhost:9092`, and **Kafka-UI** on [http://localhost:8080](http://localhost:8080) — open that in a browser to visually watch topics, partitions, messages, and consumer-group lag in real time as you use the app. It's the best companion view alongside the app's own Admin tab.

Wait ~15s for the healthcheck to pass before starting the services (`docker compose ps` should show `kafka` as healthy).

### 3. Start the 5 backend services

Each is an independent Maven project — open 5 terminals (order doesn't strictly matter, Kafka buffers everything, but starting order-service first is tidy since it declares all 4 topics):

```bash
cd order-service        && mvn spring-boot:run   # :8081
cd payment-service       && mvn spring-boot:run   # :8082
cd kitchen-service        && mvn spring-boot:run   # :8083
cd delivery-service        && mvn spring-boot:run   # :8084
cd notification-service     && mvn spring-boot:run   # :8085
```

Each prints Flyway migration logs on first boot (creating its tables) — check for `Successfully validated` / `Migrating schema` and no errors. If a service fails to start with a Flyway/DB error, double check step 1 ran and Postgres is reachable with the expected credentials.

**If Postgres rejects the connection** (`password authentication failed for user "postgres"`), your local instance just doesn't use the `postgres`/`postgres` default this project assumes. Either set env vars before running each service (`$env:DB_USERNAME='youruser'; $env:DB_PASSWORD='yourpass'; mvn spring-boot:run` in each terminal), or, if using `start-all.ps1` below, pass them once: `.\start-all.ps1 -DbUsername youruser -DbPassword yourpass`.

**Windows shortcut:** `.\start-all.ps1` launches all 5 services (+ the frontend) at once, each in its own titled PowerShell window, so you can watch logs live and stop any single one independently (Ctrl+C in its window, or `.\stop-one.ps1 -Service order-service`). `.\stop-all.ps1` shuts everything down together.
- `-NoFrontend` — skip launching the frontend.
- `-DbUsername` / `-DbPassword` — override the Postgres credentials passed to all 5 services (default `postgres`/`postgres`).
- The frontend window auto-runs `npm install` the first time if `node_modules` isn't there yet.

These just wrap the same `mvn spring-boot:run` / `npm run dev` commands above — nothing magic, just convenience.

### 4. Start the frontend

```bash
cd frontend
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173).

## Try it out

1. **Place Order** tab — pick some food items, submit. You're taken straight to **Track Order**, which opens a live SSE connection and the stepper starts advancing on its own over the next ~20-30 seconds as the backend services process the saga (payment → kitchen → delivery), each stage's timing configurable per-service (see each service's `README.md` / `application.yml` for the `*.delay-ms` properties — they're deliberately short for demo purposes).
2. Watch the **Kafka Activity** tab at the same time — every event from every topic streams in live, color-coded by topic.
3. Open **Kafka-UI** ([localhost:8080](http://localhost:8080)) in another window and watch the same messages land in the actual topics, inspect a message's raw JSON, and see consumer-group offsets move.
4. About 15% of orders will fail payment (configurable via `payment.failure-rate` in payment-service) — place a few orders to see the `CANCELLED` path too.
5. **All Orders** tab lists everything you've placed; click any row to jump back into Track Order.

### Demo the replay

1. Place 2-3 orders and let them fully complete.
2. Go to the **Admin / Replay** tab. Look at **Consumer Groups & Lag** — all groups should show ~0 lag (fully caught up).
3. Use **Event Log Lookup** with one of your order ids — every row shows `timesSeen: 1`.
4. Pick `order-status-group` from the dropdown and click **Replay from beginning**. This resets that consumer group's committed offset to the earliest available offset on `payment-events`/`kitchen-events`/`delivery-events` via the Kafka `AdminClient`.
5. Because order-service's listener is live and polling, it picks the reset up automatically within seconds and re-consumes the entire history. Look up the same order id again in **Event Log Lookup** — `timesSeen` is now `2` for every row, but the order's `status`/timeline **did not duplicate** (check the Track Order tab) — that's the idempotency guard (`ON CONFLICT ... DO UPDATE times_seen = times_seen + 1`, and status-history inserts gated on `times_seen == 1`) doing its job.
6. For a more dramatic version, try replaying `notification-service-group` instead (via `kafka-console-consumer` or restarting notification-service after resetting its group with the Admin API) and watch duplicate rows pile up in **Kafka Activity** — notification-service has no idempotency guard, by design, as a contrast.

## Project layout

```
ARCHITECTURE.md          full technical spec (source of truth)
docker-compose.yml        Kafka (KRaft) + Kafka-UI
scripts/init-db.sql        one-time Postgres provisioning
order-service/            saga initiator + finalizer, status read-model, SSE, Kafka admin/replay API   (:8081)
payment-service/           dummy payment gateway simulation                                             (:8082)
kitchen-service/            prep-stage simulation                                                        (:8083)
delivery-service/            driver/delivery simulation                                                   (:8084)
notification-service/         fan-in of all 4 topics, notification log, global SSE activity feed            (:8085)
frontend/                 Vite + React SPA                                                               (:5173)
```

Every service has its own `README.md` with its config properties and how to run it standalone.

## Notes & known limitations (by design, for a demo)

- No real payment gateway, SMS/push provider, or maps/routing — all simulated with `ScheduledExecutorService` delays and random outcomes, clearly marked in code.
- No schema registry — all Kafka payloads are plain JSON; each service keeps its own copy of the event DTOs (real independent-deployable microservices, not a shared library).
- State machine timers are in-memory (`ScheduledExecutorService`), so if you restart a service mid-flight for an order, that order's remaining scheduled transitions are lost (the order won't get "stuck" forever in the sense that Kafka still has the truth, but nothing will re-trigger the next step without a new incoming event or a real delayed-job/outbox mechanism, which is intentionally out of scope here).
- This was built and reviewed without a live Maven/Docker build (the build environment used to author it had neither available) — each service was authored and manually line-reviewed for correctness, and the frontend was `npm run build`-verified, but you should run `mvn clean compile` on each service as your first step and report back if anything doesn't compile.
