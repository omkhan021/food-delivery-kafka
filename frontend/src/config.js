export const API_BASE_ORDER =
  import.meta.env.VITE_API_BASE_ORDER || 'http://localhost:8081';

export const API_BASE_PAYMENT =
  import.meta.env.VITE_API_BASE_PAYMENT || 'http://localhost:8082';

export const API_BASE_KITCHEN =
  import.meta.env.VITE_API_BASE_KITCHEN || 'http://localhost:8083';

export const API_BASE_DELIVERY =
  import.meta.env.VITE_API_BASE_DELIVERY || 'http://localhost:8084';

export const API_BASE_NOTIFICATION =
  import.meta.env.VITE_API_BASE_NOTIFICATION || 'http://localhost:8085';

// Consumer group ids exactly as defined in ARCHITECTURE.md section 3.
export const CONSUMER_GROUPS = [
  'payment-service-group',
  'kitchen-service-group',
  'delivery-service-group',
  'order-status-group',
  'notification-service-group',
];

// Happy-path order lifecycle stages, in order, as defined in ARCHITECTURE.md
// section 5 / order-service orders.status enum (excluding the terminal
// failure states PAYMENT_FAILED / CANCELLED which are handled separately).
export const ORDER_STAGES = [
  'PLACED',
  'PAYMENT_PROCESSING',
  'RECEIVED_BY_KITCHEN',
  'PREPARING',
  'PREPARED',
  'DRIVER_ASSIGNED',
  'PICKED_UP',
  'ENROUTE',
  'DELIVERED',
  'COMPLETED',
];

// Fixed catalog of ~8 food items shown on the Place Order panel.
export const FOOD_CATALOG = [
  { itemName: 'Margherita Pizza', price: 12.99, emoji: '🍕' },
  { itemName: 'Cheeseburger', price: 9.49, emoji: '🍔' },
  { itemName: 'Caesar Salad', price: 8.25, emoji: '🥗' },
  { itemName: 'Sushi Platter', price: 18.50, emoji: '🍣' },
  { itemName: 'Pad Thai', price: 11.75, emoji: '🍜' },
  { itemName: 'Chicken Tacos', price: 9.99, emoji: '🌮' },
  { itemName: 'Veggie Burrito', price: 8.75, emoji: '🌯' },
  { itemName: 'Chocolate Lava Cake', price: 6.50, emoji: '🍫' },
];

// Distinct colors per source Kafka topic, used in the Kafka Activity console
// and can be reused elsewhere for topic-coded badges.
export const TOPIC_COLORS = {
  'order-events': '#6366f1',
  'payment-events': '#0ea5e9',
  'kitchen-events': '#f59e0b',
  'delivery-events': '#22c55e',
};

// Distinct accent color per lifecycle stage for the tracking stepper.
export const STAGE_COLORS = {
  PLACED: '#6366f1',
  PAYMENT_PROCESSING: '#0ea5e9',
  PAYMENT_FAILED: '#ef4444',
  CANCELLED: '#ef4444',
  RECEIVED_BY_KITCHEN: '#f59e0b',
  PREPARING: '#f97316',
  PREPARED: '#eab308',
  DRIVER_ASSIGNED: '#84cc16',
  PICKED_UP: '#22c55e',
  ENROUTE: '#14b8a6',
  DELIVERED: '#06b6d4',
  COMPLETED: '#8b5cf6',
};
