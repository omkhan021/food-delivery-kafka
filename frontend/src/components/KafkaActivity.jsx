import { useEffect, useRef, useState } from 'react';
import { listNotifications, notificationStreamUrl } from '../api';
import { TOPIC_COLORS } from '../config';

const MAX_ENTRIES = 150;

function formatClock(ts) {
  if (!ts) return '';
  try {
    return new Date(ts).toLocaleTimeString('en-US', { hour12: false });
  } catch {
    return ts;
  }
}

function normalize(raw, fallbackId) {
  return {
    id: raw.id ?? fallbackId,
    orderId: raw.orderId,
    sourceTopic: raw.sourceTopic,
    eventType: raw.eventType,
    message: raw.message,
    timestamp: raw.timestamp || raw.createdAt,
  };
}

export default function KafkaActivity() {
  const [entries, setEntries] = useState([]);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState(null);
  const eventSourceRef = useRef(null);
  const idCounter = useRef(0);

  useEffect(() => {
    let cancelled = false;

    async function bootstrap() {
      try {
        const history = await listNotifications();
        if (cancelled) return;
        const arr = Array.isArray(history) ? history : [];
        setEntries(arr.map((raw, idx) => normalize(raw, `hist-${idx}`)));
      } catch (err) {
        setError(err.message || 'Failed to load notification history.');
      }
    }
    bootstrap();

    const es = new EventSource(notificationStreamUrl());
    eventSourceRef.current = es;

    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);

    es.addEventListener('notification', (evt) => {
      try {
        const payload = JSON.parse(evt.data);
        idCounter.current += 1;
        const entry = normalize(payload, `live-${idCounter.current}`);
        setEntries((prev) => [entry, ...prev].slice(0, MAX_ENTRIES));
      } catch {
        /* ignore malformed event */
      }
    });

    return () => {
      cancelled = true;
      es.close();
    };
  }, []);

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Kafka Activity</h2>
        <p className="panel-subtitle">
          Live fan-in feed from notification-service — every event across all 4 topics, in real
          time.
        </p>
      </div>

      <div className="card kafka-console-card">
        <div className="table-toolbar">
          <div className="legend">
            {Object.entries(TOPIC_COLORS).map(([topic, color]) => (
              <span className="legend-item" key={topic}>
                <span className="legend-swatch" style={{ backgroundColor: color }} />
                {topic}
              </span>
            ))}
          </div>
          <span className={`conn-indicator ${connected ? 'connected' : 'disconnected'}`}>
            <span className="conn-dot" /> {connected ? 'connected' : 'connecting…'}
          </span>
        </div>

        {error && <div className="alert alert-error">{error}</div>}

        <div className="kafka-console">
          {entries.length === 0 && <p className="empty-state">Waiting for Kafka events…</p>}
          {entries.map((e) => (
            <div
              className="console-line"
              key={e.id}
              style={{ borderLeftColor: TOPIC_COLORS[e.sourceTopic] || '#64748b' }}
            >
              <span className="console-topic">[{e.sourceTopic}]</span>{' '}
              <span className="console-event">{e.eventType}</span>{' '}
              <span className="console-sep">—</span>{' '}
              <span className="console-order">{e.orderId}</span>{' '}
              <span className="console-sep">—</span>{' '}
              <span className="console-message">{e.message}</span>{' '}
              <span className="console-time">{formatClock(e.timestamp)}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
