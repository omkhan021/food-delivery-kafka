package com.fooddelivery.kitchen.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * kitchen-service's own copy of the {@code payment-events} JSON contract (see
 * ARCHITECTURE.md section 4.2). Per the architecture doc, event DTOs are NOT shared
 * across services via a common library - each service defines its own copy so the
 * services stay independently deployable.
 *
 * <p>kitchen-service only consumes this topic, so this class only needs to be
 * deserializable; it never publishes {@code payment-events}.
 *
 * <p>{@code eventType} is deliberately a plain {@code String} rather than a Java enum:
 * a String can never fail to deserialize just because a sibling service adds a new
 * event type in the future or a record has a field we don't recognize. We defensively
 * filter on the expected values (see {@code PaymentEventListener}) rather than relying
 * on deserialization to reject anything unexpected.
 *
 * <p>{@code @JsonIgnoreProperties(ignoreUnknown = true)} means this DTO tolerates any
 * extra fields present in the payload (e.g. {@code failureReason} on a
 * PAYMENT_FAILED event) without throwing.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PaymentEvent {

    private String eventId;
    private String eventType;
    private String orderId;
    private Instant timestamp;
    private String userId;
    private BigDecimal amount;
    private String transactionId;
    private String failureReason;

    public PaymentEvent() {
        // Jackson
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

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    @Override
    public String toString() {
        return "PaymentEvent{" +
                "eventId='" + eventId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", orderId='" + orderId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
