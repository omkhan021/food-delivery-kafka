package com.fooddelivery.delivery.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * Published by delivery-service to the {@code delivery-events} topic. Field shape is fixed by
 * ARCHITECTURE.md section 4.4 and must match exactly so order-service and notification-service
 * (which also deserialize a copy of this shape) stay in sync.
 *
 * <p>{@code driverId}/{@code driverName} are only populated on {@code DRIVER_ASSIGNED}.
 * {@code etaMinutes} is only populated on {@code ENROUTE}. {@code @JsonInclude(NON_NULL)} keeps
 * those fields out of the JSON entirely for the events where they don't apply, rather than
 * serializing them as {@code null}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeliveryEvent {

    private String eventId;
    private String eventType;
    private String orderId;
    private Instant timestamp;
    private String driverId;
    private String driverName;
    private Integer etaMinutes;

    public DeliveryEvent() {
    }

    public DeliveryEvent(String eventId, String eventType, String orderId, Instant timestamp,
                          String driverId, String driverName, Integer etaMinutes) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.orderId = orderId;
        this.timestamp = timestamp;
        this.driverId = driverId;
        this.driverName = driverName;
        this.etaMinutes = etaMinutes;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public Integer getEtaMinutes() {
        return etaMinutes;
    }

    public void setEtaMinutes(Integer etaMinutes) {
        this.etaMinutes = etaMinutes;
    }

    @Override
    public String toString() {
        return "DeliveryEvent{" +
                "eventId='" + eventId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", orderId='" + orderId + '\'' +
                ", timestamp=" + timestamp +
                ", driverId='" + driverId + '\'' +
                ", driverName='" + driverName + '\'' +
                ", etaMinutes=" + etaMinutes +
                '}';
    }
}
