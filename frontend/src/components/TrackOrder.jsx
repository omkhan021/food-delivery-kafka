import { useEffect, useRef, useState } from 'react';
import {
  getOrder, orderStreamUrl,
  paymentComplete, paymentFail,
  kitchenPrepare, kitchenReady,
  deliveryPickup, deliveryEnroute, deliveryDeliver,
} from '../api';
import { ORDER_STAGES, STAGE_COLORS } from '../config';

function formatTime(ts) {
  if (!ts) return '';
  try { return new Date(ts).toLocaleString(); } catch { return ts; }
}

/**
 * Shows the single action (or pair of actions) available for the current order status.
 * Each button fires the corresponding REST endpoint on the owning microservice, which
 * publishes a Kafka event. The SSE stream on order-service picks it up and pushes the
 * status update back to this component automatically.
 *
 * Button layout per status:
 *   PLACED              → (auto: payment-service will publish PAYMENT_PROCESSING shortly)
 *   PAYMENT_PROCESSING  → [✓ Pay Success]  [✗ Pay Fail]
 *   RECEIVED_BY_KITCHEN → [🍳 Start Preparing]
 *   PREPARING           → [✅ Mark Ready]
 *   PREPARED            → (auto: delivery-service will publish DRIVER_ASSIGNED shortly)
 *   DRIVER_ASSIGNED     → [📦 Picked Up]
 *   PICKED_UP           → [🚗 En Route]
 *   ENROUTE             → [🏠 Delivered]
 *   DELIVERED / COMPLETED / CANCELLED → nothing
 */
function ActionPanel({ status, orderId }) {
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState(null);

  async function trigger(apiFn) {
    setBusy(true);
    setActionError(null);
    try {
      await apiFn();
    } catch (e) {
      setActionError(e.message || 'Action failed');
    } finally {
      setBusy(false);
    }
  }

  const waiting = (msg) => (
    <div className="action-waiting">
      <span className="live-dot" /> {msg}
    </div>
  );

  let content = null;

  if (status === 'PLACED') {
    content = waiting('Waiting for payment-service to acknowledge… (PAYMENT_PROCESSING coming via Kafka)');
  } else if (status === 'PAYMENT_PROCESSING') {
    content = (
      <>
        <p className="action-hint">
          Simulate the payment gateway result — watch the event land in Kafka-UI and the status update via SSE:
        </p>
        <div className="action-buttons">
          <button
            className="btn btn-success action-btn"
            disabled={busy}
            onClick={() => trigger(() => paymentComplete(orderId))}
          >
            ✓ Pay Success
          </button>
          <button
            className="btn btn-danger action-btn"
            disabled={busy}
            onClick={() => trigger(() => paymentFail(orderId))}
          >
            ✗ Pay Fail
          </button>
        </div>
      </>
    );
  } else if (status === 'RECEIVED_BY_KITCHEN') {
    content = (
      <button
        className="btn btn-primary action-btn"
        disabled={busy}
        onClick={() => trigger(() => kitchenPrepare(orderId))}
      >
        🍳 Start Preparing
      </button>
    );
  } else if (status === 'PREPARING') {
    content = (
      <button
        className="btn btn-primary action-btn"
        disabled={busy}
        onClick={() => trigger(() => kitchenReady(orderId))}
      >
        ✅ Mark Ready
      </button>
    );
  } else if (status === 'PREPARED') {
    content = waiting('Waiting for delivery-service to assign a driver… (DRIVER_ASSIGNED coming via Kafka)');
  } else if (status === 'DRIVER_ASSIGNED') {
    content = (
      <button
        className="btn btn-primary action-btn"
        disabled={busy}
        onClick={() => trigger(() => deliveryPickup(orderId))}
      >
        📦 Picked Up
      </button>
    );
  } else if (status === 'PICKED_UP') {
    content = (
      <button
        className="btn btn-primary action-btn"
        disabled={busy}
        onClick={() => trigger(() => deliveryEnroute(orderId))}
      >
        🚗 En Route
      </button>
    );
  } else if (status === 'ENROUTE') {
    content = (
      <button
        className="btn btn-primary action-btn"
        disabled={busy}
        onClick={() => trigger(() => deliveryDeliver(orderId))}
      >
        🏠 Delivered
      </button>
    );
  } else if (status === 'DELIVERED' || status === 'COMPLETED') {
    content = <div className="action-waiting">✓ Order complete — no further actions.</div>;
  }

  if (!content) return null;

  return (
    <div className="card action-panel">
      <h3 className="card-title">⚡ Next Action</h3>
      {content}
      {busy && <div className="action-waiting">Sending to Kafka…</div>}
      {actionError && <div className="alert alert-error">{actionError}</div>}
    </div>
  );
}

export default function TrackOrder({ orderId, setOrderId }) {
  const [inputValue, setInputValue] = useState(orderId || '');
  const [order, setOrder] = useState(null);
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [liveStatus, setLiveStatus] = useState(null);
  const eventSourceRef = useRef(null);

  useEffect(() => {
    setInputValue(orderId || '');
    if (orderId) loadOrder(orderId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [orderId]);

  useEffect(() => {
    return () => { if (eventSourceRef.current) eventSourceRef.current.close(); };
  }, []);

  function connectStream(id) {
    if (eventSourceRef.current) { eventSourceRef.current.close(); eventSourceRef.current = null; }
    if (!id) return;
    const es = new EventSource(orderStreamUrl(id));
    eventSourceRef.current = es;
    es.addEventListener('status', (evt) => {
      try {
        const payload = JSON.parse(evt.data);
        setLiveStatus(payload);
        setOrder((prev) => (prev ? { ...prev, status: payload.status } : prev));
        setHistory((prev) => [
          ...prev,
          { status: payload.status, note: payload.note, createdAt: payload.timestamp, sourceTopic: payload.sourceTopic },
        ]);
      } catch { /* ignore malformed */ }
    });
    es.onerror = () => {};
  }

  async function loadOrder(id) {
    if (!id) return;
    setLoading(true);
    setError(null);
    setLiveStatus(null);
    try {
      const data = await getOrder(id);
      setOrder(data);
      setHistory(data?.statusHistory || data?.history || data?.orderStatusHistory || []);
      connectStream(id);
    } catch (err) {
      setError(err.message || 'Failed to load order.');
      setOrder(null);
      setHistory([]);
    } finally {
      setLoading(false);
    }
  }

  function handleLoadClick() {
    const id = inputValue.trim();
    if (!id) return;
    setOrderId(id);
    loadOrder(id);
  }

  const status = order?.status;
  const isCancelled = status === 'CANCELLED' || status === 'PAYMENT_FAILED';
  const currentStageIndex = ORDER_STAGES.indexOf(status);

  const cancelReasonEntry = [...history].reverse().find((h) => h.note || h.reason);
  const cancelReason = cancelReasonEntry?.note || cancelReasonEntry?.reason || 'No reason provided.';

  const chronological = [...history].sort((a, b) => {
    const ta = new Date(a.createdAt || a.timestamp || 0).getTime();
    const tb = new Date(b.createdAt || b.timestamp || 0).getTime();
    return ta - tb;
  });

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Track Order</h2>
        <p className="panel-subtitle">
          Step through the order lifecycle manually — each button publishes a Kafka event to the
          owning microservice. Watch it appear in Kafka-UI and the status update via SSE.
        </p>
      </div>

      <div className="card track-input-card">
        <label className="field-label" htmlFor="orderIdInput">Order ID</label>
        <div className="track-input-row">
          <input
            id="orderIdInput"
            type="text"
            className="text-input"
            placeholder="ORD-xxxxxxxx"
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleLoadClick()}
          />
          <button className="btn btn-primary" onClick={handleLoadClick} disabled={loading}>
            {loading ? 'Loading…' : 'Load'}
          </button>
        </div>
        {error && <div className="alert alert-error">{error}</div>}
      </div>

      {order && (
        <>
          <div className="card order-meta-card">
            <div className="order-meta-row">
              <div>
                <div className="meta-label">Order ID</div>
                <div className="meta-value">{order.orderId}</div>
              </div>
              <div>
                <div className="meta-label">User</div>
                <div className="meta-value">{order.userId}</div>
              </div>
              <div>
                <div className="meta-label">Amount</div>
                <div className="meta-value">${Number(order.paymentAmount ?? order.amount ?? 0).toFixed(2)}</div>
              </div>
              <div>
                <div className="meta-label">Status</div>
                <div
                  className="meta-value status-pill"
                  style={{ backgroundColor: STAGE_COLORS[status] || '#64748b' }}
                >
                  {status}
                </div>
              </div>
            </div>
            {liveStatus && (
              <div className="live-indicator">
                <span className="live-dot" /> live update received at {formatTime(liveStatus.timestamp)}
              </div>
            )}
          </div>

          {/* Action panel — shown above the stepper so it's the first thing you see */}
          {!isCancelled && (
            <ActionPanel status={status} orderId={order.orderId} />
          )}

          {isCancelled ? (
            <div className="card cancelled-banner">
              <div className="cancelled-title">⚠ Order Cancelled</div>
              <div className="cancelled-reason">{cancelReason}</div>
            </div>
          ) : (
            <div className="card stepper-card">
              <div className="stepper">
                {ORDER_STAGES.map((stage, idx) => {
                  const completed = currentStageIndex > idx;
                  const current = currentStageIndex === idx;
                  const color = STAGE_COLORS[stage] || '#64748b';
                  return (
                    <div className="stepper-item" key={stage}>
                      <div
                        className={`stepper-dot ${completed ? 'completed' : ''} ${current ? 'current' : ''}`}
                        style={{
                          backgroundColor: completed || current ? color : undefined,
                          borderColor: color,
                        }}
                      >
                        {completed ? '✓' : idx + 1}
                      </div>
                      <div className={`stepper-label ${current ? 'current-label' : ''}`}>
                        {stage.replaceAll('_', ' ')}
                      </div>
                      {idx < ORDER_STAGES.length - 1 && (
                        <div
                          className={`stepper-line ${completed ? 'completed' : ''}`}
                          style={{ backgroundColor: completed ? color : undefined }}
                        />
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          <div className="card timeline-card">
            <h3 className="card-title">Timeline (chronological)</h3>
            {chronological.length === 0 && <p className="empty-state">No history yet.</p>}
            <ul className="timeline-list">
              {chronological.map((h, idx) => (
                <li key={idx} className="timeline-item">
                  <span
                    className="timeline-dot"
                    style={{ backgroundColor: STAGE_COLORS[h.status] || '#64748b' }}
                  />
                  <div className="timeline-body">
                    <div className="timeline-status">{h.status}</div>
                    {h.note && <div className="timeline-note">{h.note}</div>}
                    {h.sourceTopic && <div className="timeline-topic">source: {h.sourceTopic}</div>}
                  </div>
                  <div className="timeline-time">{formatTime(h.createdAt || h.timestamp)}</div>
                </li>
              ))}
            </ul>
          </div>
        </>
      )}
    </div>
  );
}
