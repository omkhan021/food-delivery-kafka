package com.fooddelivery.order.dto.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

/**
 * Event published by order-service (the only producer of {@code order-events}) to
 * {@code order-events}. eventType is one of ORDER_PLACED | ORDER_COMPLETED | ORDER_CANCELLED.
 *
 * <p>Per ARCHITECTURE.md section 4.1, this class is independently duplicated in every service
 * that needs to read {@code order-events} (no shared library module across services), so field
 * names/shape here must stay byte-for-byte in sync with the spec.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {
    private String eventId;
    private String eventType;
    private String orderId;
    private Instant timestamp;

    // Only populated on ORDER_PLACED:
    private String userId;
    private BigDecimal paymentAmount;
    private List<FoodItem> foodItems;
    private LocalDate orderDate;
    private LocalTime orderTime;

    // Only populated on ORDER_CANCELLED:
    private String reason;
}
