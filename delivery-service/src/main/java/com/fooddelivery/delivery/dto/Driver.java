package com.fooddelivery.delivery.dto;

/**
 * A dummy driver from the fixed in-memory roster. There is no drivers table/service in this
 * demo — a real system would look up an available driver from a fleet-management service, but
 * for the purposes of this Kafka demo we just pick one at random from
 * {@link com.fooddelivery.delivery.service.DriverRoster}.
 */
public record Driver(String id, String name) {
}
