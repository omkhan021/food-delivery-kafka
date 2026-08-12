import { useEffect, useState } from 'react';
import { listOrders } from '../api';
import { STAGE_COLORS } from '../config';

function formatTime(ts) {
  if (!ts) return '';
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return ts;
  }
}

export default function AllOrders({ onSelectOrder }) {
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const data = await listOrders();
      setOrders(Array.isArray(data) ? data : []);
    } catch (err) {
      setError(err.message || 'Failed to load orders.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>All Orders</h2>
        <p className="panel-subtitle">Every order placed, newest first. Click a row to track it.</p>
      </div>

      <div className="card">
        <div className="table-toolbar">
          <button className="btn btn-secondary" onClick={load} disabled={loading}>
            {loading ? 'Refreshing…' : '↻ Refresh'}
          </button>
        </div>

        {error && <div className="alert alert-error">{error}</div>}

        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Order ID</th>
                <th>User ID</th>
                <th>Amount</th>
                <th>Status</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {orders.length === 0 && !loading && (
                <tr>
                  <td colSpan={5} className="empty-state">
                    No orders yet — place one from the Place Order tab.
                  </td>
                </tr>
              )}
              {orders.map((o) => (
                <tr
                  key={o.orderId}
                  className="clickable-row"
                  onClick={() => onSelectOrder(o.orderId)}
                >
                  <td className="mono">{o.orderId}</td>
                  <td>{o.userId}</td>
                  <td>${Number(o.paymentAmount ?? o.amount ?? 0).toFixed(2)}</td>
                  <td>
                    <span
                      className="status-pill small"
                      style={{ backgroundColor: STAGE_COLORS[o.status] || '#64748b' }}
                    >
                      {o.status}
                    </span>
                  </td>
                  <td>{formatTime(o.createdAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
