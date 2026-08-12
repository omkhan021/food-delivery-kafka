package com.fooddelivery.order.dto.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Mirrors kitchen-service's {@code kitchen-events} contract (ARCHITECTURE.md section 4.3).
 * eventType is one of ORDER_RECEIVED | PREPARING | PREPARED. See {@link PaymentEvent} javadoc
 * for why the live listener parses these generically instead of via this POJO.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class KitchenEvent {
    private String eventId;
    private String eventType;
    private String orderId;
    private Instant timestamp;
    private Integer estimatedMinutes;
}
