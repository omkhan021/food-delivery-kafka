package com.fooddelivery.order.domain;

/**
 * The full order status lifecycle, exactly as enumerated in ARCHITECTURE.md section 5.
 * Stored as its {@link #name()} string in {@code orders.status} (VARCHAR(32)).
 */
public enum OrderStatus {
    PLACED,
    PAYMENT_PROCESSING,
    PAYMENT_FAILED,
    CANCELLED,
    RECEIVED_BY_KITCHEN,
    PREPARING,
    PREPARED,
    DRIVER_ASSIGNED,
    PICKED_UP,
    ENROUTE,
    DELIVERED,
    COMPLETED
}
