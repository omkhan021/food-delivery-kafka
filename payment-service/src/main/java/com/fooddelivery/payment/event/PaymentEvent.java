package com.fooddelivery.payment.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The {@code payment-events} contract, defined in ARCHITECTURE.md section 4.2. Field names must
 * match exactly since every other service (and the frontend, indirectly via order-service/
 * notification-service) deserializes this exact shape.
 *
 * {@code transactionId} is only populated on PAYMENT_COMPLETED, {@code failureReason} only on
 * PAYMENT_FAILED - @JsonInclude(NON_NULL) keeps those fields out of the JSON entirely otherwise
 * (also enforced globally via spring.jackson.default-property-inclusion=non_null).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
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
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", transactionId='" + transactionId + '\'' +
                ", failureReason='" + failureReason + '\'' +
                '}';
    }
}
