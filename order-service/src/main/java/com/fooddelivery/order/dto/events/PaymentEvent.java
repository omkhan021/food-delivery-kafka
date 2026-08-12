package com.fooddelivery.order.dto.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Mirrors payment-service's {@code payment-events} contract (ARCHITECTURE.md section 4.2).
 * eventType is one of PAYMENT_PROCESSING | PAYMENT_COMPLETED | PAYMENT_FAILED.
 *
 * <p>Kept here for documentation/reference and for tests. The live
 * {@code KafkaEventConsumerService} listener deserializes incoming records generically via
 * Jackson's {@code JsonNode} instead of this POJO (see the value-deserializer comment in
 * application.yml) so that one {@code @KafkaListener} method can cleanly handle 3 different
 * event shapes without needing 3 separate listener containers.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    private String eventId;
    private String eventType;
    private String orderId;
    private Instant timestamp;
    private String userId;
    private BigDecimal amount;
    private String transactionId;
    private String failureReason;
}
