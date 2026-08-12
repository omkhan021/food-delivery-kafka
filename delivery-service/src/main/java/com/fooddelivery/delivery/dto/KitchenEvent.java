package com.fooddelivery.delivery.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * Mirrors kitchen-service's {@code KitchenEvent} published to the {@code kitchen-events} topic.
 *
 * <p>Per the architecture contract, every microservice keeps its own private copy of the event
 * DTOs it needs (no shared library module between independently-deployable services). This
 * class only needs to be able to deserialize the fields delivery-service actually cares about;
 * {@code estimatedMinutes} is only ever populated on {@code PREPARING} events, which this
 * service ignores anyway.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class KitchenEvent {

    private String eventId;
    private String eventType;
    private String orderId;
    private Instant timestamp;
    private Integer estimatedMinutes;

    public KitchenEvent() {
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

    public Integer getEstimatedMinutes() {
        return estimatedMinutes;
    }

    public void setEstimatedMinutes(Integer estimatedMinutes) {
        this.estimatedMinutes = estimatedMinutes;
    }

    @Override
    public String toString() {
        return "KitchenEvent{" +
                "eventId='" + eventId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", orderId='" + orderId + '\'' +
                ", timestamp=" + timestamp +
                ", estimatedMinutes=" + estimatedMinutes +
                '}';
    }
}
