import { API_BASE_ORDER, API_BASE_PAYMENT, API_BASE_KITCHEN, API_BASE_DELIVERY, API_BASE_NOTIFICATION } from './config';

async function handle(res) {
  if (!res.ok) {
    let detail = '';
    try {
      detail = await res.text();
    } catch {
      /* ignore */
    }
    throw new Error(`${res.status} ${res.statusText}${detail ? ` — ${detail}` : ''}`);
  }
  if (res.status === 204) return null;
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

// ---- order-service ----

export function placeOrder(body) {
  return fetch(`${API_BASE_ORDER}/api/orders`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }).then(handle);
}

export function listOrders() {
  return fetch(`${API_BASE_ORDER}/api/orders`).then(handle);
}

export function getOrder(orderId) {
  return fetch(`${API_BASE_ORDER}/api/orders/${encodeURIComponent(orderId)}`).then(handle);
}

export function getOrderEventLog(orderId) {
  return fetch(`${API_BASE_ORDER}/api/orders/${encodeURIComponent(orderId)}/event-log`).then(handle);
}

export function getKafkaTopics() {
  return fetch(`${API_BASE_ORDER}/api/admin/kafka/topics`).then(handle);
}

export function getConsumerGroups() {
  return fetch(`${API_BASE_ORDER}/api/admin/kafka/consumer-groups`).then(handle);
}

export function replayFromBeginning(listenerId) {
  return fetch(`${API_BASE_ORDER}/api/admin/kafka/replay`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ listenerId }),
  }).then(handle);
}

export function orderStreamUrl(orderId) {
  return `${API_BASE_ORDER}/api/orders/${encodeURIComponent(orderId)}/stream`;
}

// ---- manual stage triggers ----

export function paymentComplete(orderId) {
  return fetch(`${API_BASE_PAYMENT}/api/payments/${encodeURIComponent(orderId)}/complete`, {
    method: 'POST',
  }).then(handle);
}

export function paymentFail(orderId) {
  return fetch(`${API_BASE_PAYMENT}/api/payments/${encodeURIComponent(orderId)}/fail`, {
    method: 'POST',
  }).then(handle);
}

export function kitchenPrepare(orderId) {
  return fetch(`${API_BASE_KITCHEN}/api/kitchen/${encodeURIComponent(orderId)}/prepare`, {
    method: 'POST',
  }).then(handle);
}

export function kitchenReady(orderId) {
  return fetch(`${API_BASE_KITCHEN}/api/kitchen/${encodeURIComponent(orderId)}/ready`, {
    method: 'POST',
  }).then(handle);
}

export function deliveryPickup(orderId) {
  return fetch(`${API_BASE_DELIVERY}/api/deliveries/${encodeURIComponent(orderId)}/pickup`, {
    method: 'POST',
  }).then(handle);
}

export function deliveryEnroute(orderId) {
  return fetch(`${API_BASE_DELIVERY}/api/deliveries/${encodeURIComponent(orderId)}/enroute`, {
    method: 'POST',
  }).then(handle);
}

export function deliveryDeliver(orderId) {
  return fetch(`${API_BASE_DELIVERY}/api/deliveries/${encodeURIComponent(orderId)}/deliver`, {
    method: 'POST',
  }).then(handle);
}

// ---- notification-service ----

export function listNotifications() {
  return fetch(`${API_BASE_NOTIFICATION}/api/notifications`).then(handle);
}

export function notificationStreamUrl() {
  return `${API_BASE_NOTIFICATION}/api/notifications/stream`;
}
