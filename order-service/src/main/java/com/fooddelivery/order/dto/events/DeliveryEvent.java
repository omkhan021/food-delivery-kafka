package com.fooddelivery.order.dto.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Mirrors delivery-service's {@code delivery-events} contract (ARCHITECTURE.md section 4.4).
 * eventType is one of DRIVER_ASSIGNED | PICKED_UP | ENROUTE | DELIVERED. See
 * {@link PaymentEvent} javadoc for why the live listener parses these generically instead of via
 * this POJO.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DeliveryEvent {
    private String eventId;
    private String eventType;
    private String orderId;
    private Instant timestamp;
    private String driverId;
    private String driverName;
    private Integer etaMinutes;
}
