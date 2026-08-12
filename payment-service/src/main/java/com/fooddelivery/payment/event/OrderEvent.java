package com.fooddelivery.payment.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

/**
 * payment-service's own copy of the {@code order-events} / {@code OrderEvent} contract defined
 * in ARCHITECTURE.md section 4.1. Per section 3, each service keeps an independent copy of the
 * shared DTO classes rather than depending on a shared library module, so that services remain
 * independently deployable.
 *
 * payment-service only cares about {@code eventType == ORDER_PLACED} events, and only reads
 * {@code orderId}, {@code userId} and {@code paymentAmount} from them - the rest of the fields
 * are declared here purely so the full JSON payload deserializes cleanly.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OrderEvent {

    private String eventId;
    private String eventType;
    private String orderId;
    private Instant timestamp;
    private String userId;
    private BigDecimal paymentAmount;
    private List<FoodItem> foodItems;
    private LocalDate orderDate;
    private LocalTime orderTime;
    private String reason;

    public OrderEvent() {
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

    public BigDecimal getPaymentAmount() {
        return paymentAmount;
    }

    public void setPaymentAmount(BigDecimal paymentAmount) {
        this.paymentAmount = paymentAmount;
    }

    public List<FoodItem> getFoodItems() {
        return foodItems;
    }

    public void setFoodItems(List<FoodItem> foodItems) {
        this.foodItems = foodItems;
    }

    public LocalDate getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(LocalDate orderDate) {
        this.orderDate = orderDate;
    }

    public LocalTime getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(LocalTime orderTime) {
        this.orderTime = orderTime;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "eventId='" + eventId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", paymentAmount=" + paymentAmount +
                '}';
    }
}
