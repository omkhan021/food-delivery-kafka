import { useState } from 'react';
import PlaceOrder from './components/PlaceOrder.jsx';
import TrackOrder from './components/TrackOrder.jsx';
import AllOrders from './components/AllOrders.jsx';
import KafkaActivity from './components/KafkaActivity.jsx';
import AdminReplay from './components/AdminReplay.jsx';

const TABS = [
  { id: 'place', label: 'Place Order', icon: '🛒' },
  { id: 'track', label: 'Track Order', icon: '📍' },
  { id: 'all', label: 'All Orders', icon: '📋' },
  { id: 'kafka', label: 'Kafka Activity', icon: '📡' },
  { id: 'admin', label: 'Admin / Replay', icon: '⚙️' },
];

export default function App() {
  const [activeTab, setActiveTab] = useState('place');
  // Shared "which order id is being tracked" state, so Place Order / All
  // Orders can hand off to the Track Order tab.
  const [trackedOrderId, setTrackedOrderId] = useState('');

  function goTrack(orderId) {
    setTrackedOrderId(orderId);
    setActiveTab('track');
  }

  return (
    <div className="app-shell">
      <header className="top-nav">
        <div className="brand">
          <span className="brand-emoji">🍔</span>
          <span className="brand-name">FoodStream</span>
          <span className="brand-tag">Kafka Delivery Demo</span>
        </div>
        <nav className="tab-bar">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              className={`tab-btn ${activeTab === tab.id ? 'active' : ''}`}
              onClick={() => setActiveTab(tab.id)}
            >
              <span className="tab-icon">{tab.icon}</span>
              {tab.label}
            </button>
          ))}
        </nav>
      </header>

      <main className="tab-content">
        {activeTab === 'place' && <PlaceOrder onOrderPlaced={goTrack} />}
        {activeTab === 'track' && (
          <TrackOrder orderId={trackedOrderId} setOrderId={setTrackedOrderId} />
        )}
        {activeTab === 'all' && <AllOrders onSelectOrder={goTrack} />}
        {activeTab === 'kafka' && <KafkaActivity />}
        {activeTab === 'admin' && <AdminReplay />}
      </main>

      <footer className="app-footer">
        Food-Ordering-and-Delivery Kafka Demo &mdash; order-service :8081 &middot;
        notification-service :8085
      </footer>
    </div>
  );
}
