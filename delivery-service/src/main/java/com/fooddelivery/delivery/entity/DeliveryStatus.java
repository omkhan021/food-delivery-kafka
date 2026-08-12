package com.fooddelivery.delivery.entity;

/**
 * The 4 stages a delivery moves through once a driver is assigned. Mirrors the
 * {@code DeliveryEvent.eventType} values published to {@code delivery-events} (minus the
 * "EVENT" framing — the DB column stores the delivery's current state, the Kafka event
 * describes the state *transition*).
 */
public enum DeliveryStatus {
    ASSIGNED,
    PICKED_UP,
    ENROUTE,
    DELIVERED
}
