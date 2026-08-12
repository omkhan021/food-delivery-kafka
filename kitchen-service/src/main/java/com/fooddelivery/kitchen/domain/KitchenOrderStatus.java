package com.fooddelivery.kitchen.domain;

/**
 * Local persistence status for a {@code kitchen_orders} row. Mirrors the three
 * {@code KitchenEvent.eventType} values this service ever publishes
 * (ORDER_RECEIVED -> RECEIVED, PREPARING -> PREPARING, PREPARED -> PREPARED).
 */
public enum KitchenOrderStatus {
    RECEIVED,
    PREPARING,
    PREPARED
}
