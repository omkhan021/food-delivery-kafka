import { useMemo, useState } from 'react';
import { FOOD_CATALOG } from '../config';
import { placeOrder } from '../api';

export default function PlaceOrder({ onOrderPlaced }) {
  const [userId, setUserId] = useState('user-101');
  const [quantities, setQuantities] = useState({});
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);
  const [lastOrder, setLastOrder] = useState(null);

  const total = useMemo(() => {
    return FOOD_CATALOG.reduce((sum, item) => {
      const qty = quantities[item.itemName] || 0;
      return sum + qty * item.price;
    }, 0);
  }, [quantities]);

  const itemCount = useMemo(
    () => Object.values(quantities).reduce((a, b) => a + b, 0),
    [quantities],
  );

  function setQty(itemName, qty) {
    setQuantities((prev) => ({ ...prev, [itemName]: Math.max(0, qty) }));
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setError(null);

    const foodItems = FOOD_CATALOG.filter((item) => (quantities[item.itemName] || 0) > 0).map(
      (item) => ({
        itemName: item.itemName,
        quantity: quantities[item.itemName],
        price: item.price,
      }),
    );

    if (!userId.trim()) {
      setError('Please enter a user id.');
      return;
    }
    if (foodItems.length === 0) {
      setError('Please select at least one item.');
      return;
    }

    setSubmitting(true);
    try {
      const created = await placeOrder({ userId: userId.trim(), foodItems });
      setLastOrder(created);
      setQuantities({});
      if (created?.orderId) {
        onOrderPlaced(created.orderId);
      }
    } catch (err) {
      setError(err.message || 'Failed to place order.');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Place an Order</h2>
        <p className="panel-subtitle">
          Pick your items — this kicks off the full Kafka saga: order → payment → kitchen →
          delivery.
        </p>
      </div>

      <form onSubmit={handleSubmit} className="place-order-form">
        <div className="card">
          <label className="field-label" htmlFor="userId">
            User ID
          </label>
          <input
            id="userId"
            type="text"
            className="text-input"
            value={userId}
            onChange={(e) => setUserId(e.target.value)}
            placeholder="user-101"
          />
        </div>

        <div className="card catalog-card">
          <h3 className="card-title">Menu</h3>
          <div className="catalog-grid">
            {FOOD_CATALOG.map((item) => {
              const qty = quantities[item.itemName] || 0;
              return (
                <div className={`catalog-item ${qty > 0 ? 'selected' : ''}`} key={item.itemName}>
                  <div className="catalog-item-emoji">{item.emoji}</div>
                  <div className="catalog-item-info">
                    <div className="catalog-item-name">{item.itemName}</div>
                    <div className="catalog-item-price">${item.price.toFixed(2)}</div>
                  </div>
                  <div className="qty-stepper">
                    <button
                      type="button"
                      className="qty-btn"
                      onClick={() => setQty(item.itemName, qty - 1)}
                      disabled={qty === 0}
                    >
                      −
                    </button>
                    <span className="qty-value">{qty}</span>
                    <button
                      type="button"
                      className="qty-btn"
                      onClick={() => setQty(item.itemName, qty + 1)}
                    >
                      +
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div className="card order-summary-card">
          <div className="order-summary-row">
            <span>Items</span>
            <span>{itemCount}</span>
          </div>
          <div className="order-summary-row total-row">
            <span>Total</span>
            <span>${total.toFixed(2)}</span>
          </div>
          {error && <div className="alert alert-error">{error}</div>}
          <button type="submit" className="btn btn-primary btn-block" disabled={submitting}>
            {submitting ? 'Placing order…' : 'Place Order'}
          </button>
          {lastOrder && (
            <div className="alert alert-success">
              Order <strong>{lastOrder.orderId}</strong> placed! Redirecting to tracking…
            </div>
          )}
        </div>
      </form>
    </div>
  );
}
