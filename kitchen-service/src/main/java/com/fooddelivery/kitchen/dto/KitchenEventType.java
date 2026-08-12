package com.fooddelivery.kitchen.dto;

/**
 * The three {@code eventType} values kitchen-service publishes to {@code kitchen-events}
 * (ARCHITECTURE.md section 4.3). Jackson serializes this as the plain enum name
 * (e.g. {@code "PREPARING"}), matching the contract exactly.
 */
public enum KitchenEventType {
    ORDER_RECEIVED,
    PREPARING,
    PREPARED
}
