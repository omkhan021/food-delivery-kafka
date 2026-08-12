# Food Delivery Kafka Demo — Frontend

A Vite + React single-page app for the Food-Ordering-and-Delivery Kafka demo. Consumes the
`order-service` (port 8081) and `notification-service` (port 8085) REST/SSE APIs described in
`ARCHITECTURE.md` at the repo root.

## Panels

1. **Place Order** — pick items from a fixed menu, submit, hands off to Track Order with the new order id.
2. **Track Order** — order id lookup + live SSE-fed horizontal stepper (`PLACED` → ... → `COMPLETED`, or a `CANCELLED` banner) with a full status timeline below.
3. **All Orders** — table of every order, click a row to track it.
4. **Kafka Activity** — live scrolling console of every event across all 4 Kafka topics, fed by notification-service's global SSE feed, color-coded by topic.
5. **Admin / Replay** — Kafka topic/consumer-group inspector, "replay from beginning" trigger, and an event-log lookup that surfaces the `timesSeen` counter (the visible proof that a replay caused reprocessing).

## Run instructions

```bash
npm install
npm run dev
```

The dev server runs on **http://localhost:5173** (matches the CORS allowlist configured on every backend service).

To build a production bundle:

```bash
npm run build
npm run preview   # optional local preview of the dist/ build
```

## Environment variables

Copy `.env.example` to `.env` (or `.env.local`) and adjust if your backend services run on
different hosts/ports:

| Variable                       | Default                  | Used for                                                        |
|--------------------------------|---------------------------|-------------------------------------------------------------------|
| `VITE_API_BASE_ORDER`            | `http://localhost:8081`    | order-service: place/list/get orders, tracking SSE, admin/replay APIs |
| `VITE_API_BASE_NOTIFICATION`      | `http://localhost:8085`    | notification-service: notification history + global activity SSE feed |

Both variables have sane localhost defaults baked into `src/config.js`, so the app runs
out-of-the-box against the default backend ports even without a `.env` file.

## Notes on the Admin / Replay panel

Triggering "Replay from beginning" resets a consumer group's committed offsets to the earliest
available offset on its subscribed topic(s) via the order-service's `AdminClient`-backed replay
endpoint. If the target backend service's `@KafkaListener` is actively polling, it may pick up the
reset immediately; otherwise — or for a guaranteed clean replay — restart that backend service
process afterward so its listener reconnects and re-consumes from the reset offset. This is normal
Kafka operational practice, not a bug. After a successful replay + reprocessing, look up the
affected order id in the **Event Log Lookup** box below and watch `timesSeen` increment (1 → 2,
etc.) per re-delivered offset.

## Tech choices

- Plain JS/JSX functional components + hooks (no TypeScript, no router library — a simple
  `useState` tab switcher in `src/App.jsx`).
- `fetch` for REST calls (`src/api.js`), browser `EventSource` for SSE streams.
- Plain CSS (`src/App.css`) — no Tailwind or UI kit, kept dependency footprint to React + Vite only.
