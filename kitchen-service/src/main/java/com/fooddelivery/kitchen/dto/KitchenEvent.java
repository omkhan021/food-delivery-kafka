package com.fooddelivery.kitchen.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;

/**
 * The {@code kitchen-events} JSON contract this service publishes (ARCHITECTURE.md
 * section 4.3). Exact field names matter: other services (order-service,
 * delivery-service, notification-service, and the frontend) all deserialize this
 * same shape, so it must match byte-for-byte with what the rest of the project expects:
 *
 * <pre>
 * { "eventId":"uuid", "eventType":"ORDER_RECEIVED | PREPARING | PREPARED",
 *   "orderId":"...", "timestamp":"...", "estimatedMinutes": 15 }
 * </pre>
 *
 * {@code estimatedMinutes} is only ever populated on a {@code PREPARING} event;
 * {@code @JsonInclude(NON_NULL)} means it is simply omitted from the JSON for
 * ORDER_RECEIVED / PREPARED, exactly as the contract specifies, rather than being
 * serialized as {@code null}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KitchenEvent {

    private String eventId;
    private KitchenEventType eventType;
    private String orderId;
    private Instant timestamp;
    private Integer estimatedMinutes;

    public KitchenEvent() {
        // Jackson
    }

    private KitchenEvent(KitchenEventType eventType, String orderId, Integer estimatedMinutes) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.orderId = orderId;
        this.timestamp = Instant.now();
        this.estimatedMinutes = estimatedMinutes;
    }

    public static KitchenEvent orderReceived(String orderId) {
        return new KitchenEvent(KitchenEventType.ORDER_RECEIVED, orderId, null);
    }

    public static KitchenEvent preparing(String orderId, int estimatedMinutes) {
        return new KitchenEvent(KitchenEventType.PREPARING, orderId, estimatedMinutes);
    }

    public static KitchenEvent prepared(String orderId) {
        return new KitchenEvent(KitchenEventType.PREPARED, orderId, null);
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public KitchenEventType getEventType() {
        return eventType;
    }

    public void setEventType(KitchenEventType eventType) {
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
                ", eventType=" + eventType +
                ", orderId='" + orderId + '\'' +
                ", timestamp=" + timestamp +
                ", estimatedMinutes=" + estimatedMinutes +
                '}';
    }
}
