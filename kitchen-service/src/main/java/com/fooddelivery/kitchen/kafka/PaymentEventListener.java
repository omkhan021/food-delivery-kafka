package com.fooddelivery.kitchen.kafka;

import com.fooddelivery.kitchen.dto.PaymentEvent;
import com.fooddelivery.kitchen.service.KitchenOrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code payment-events} as consumer group {@code kitchen-service-group}
 * (id and groupId must match exactly - the order-service replay admin API looks up
 * this listener container by that id via {@code KafkaListenerEndpointRegistry}).
 *
 * <p>{@code payment-events} carries three event types (PAYMENT_PROCESSING,
 * PAYMENT_COMPLETED, PAYMENT_FAILED) but kitchen-service only cares about the
 * terminal success case - it has nothing to do until a payment has actually gone
 * through. Every other eventType is explicitly ignored below rather than left
 * unhandled, so it's clear in the code (and the logs) that they were seen and
 * deliberately skipped, not missed.
 */
@Component
public class PaymentEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventListener.class);

    private static final String EVENT_TYPE_PAYMENT_COMPLETED = "PAYMENT_COMPLETED";

    private final KitchenOrderService kitchenOrderService;

    public PaymentEventListener(KitchenOrderService kitchenOrderService) {
        this.kitchenOrderService = kitchenOrderService;
    }

    @KafkaListener(
            id = "kitchen-service-group",
            groupId = "kitchen-service-group",
            topics = "payment-events")
    public void onPaymentEvent(
            @Payload(required = false) PaymentEvent event,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        // Defensive catch-all: this listener must never let an exception escape and
        // crash the consumer thread / poison the payment-events partition. Pure
        // deserialization failures (non-JSON bytes, totally wrong shape) never even
        // reach this method - they're caught earlier by the ErrorHandlingDeserializer +
        // CommonErrorHandler configured in KafkaConsumerErrorConfig. This try/catch
        // instead guards against a *valid* PaymentEvent that fails business processing
        // (e.g. an unexpected null field, or a transient DB error while persisting).
        try {
            if (event == null) {
                log.warn("Received null/unreadable payment-events record key={} partition={} offset={}, skipping", key, partition, offset);
                return;
            }

            log.debug("Received payment-events record key={} partition={} offset={} eventType={} orderId={}",
                    key, partition, offset, event.getEventType(), event.getOrderId());

            if (!EVENT_TYPE_PAYMENT_COMPLETED.equals(event.getEventType())) {
                // Ignore PAYMENT_PROCESSING, PAYMENT_FAILED, and any future/unknown
                // eventType - kitchen-service only reacts to PAYMENT_COMPLETED.
                log.debug("Ignoring payment-events eventType={} for orderId={} (only PAYMENT_COMPLETED starts kitchen prep)",
                        event.getEventType(), event.getOrderId());
                return;
            }

            if (event.getOrderId() == null || event.getOrderId().isBlank()) {
                log.warn("PAYMENT_COMPLETED event is missing orderId, skipping: {}", event);
                return;
            }

            kitchenOrderService.handlePaymentCompleted(event.getOrderId());
        } catch (Exception e) {
            log.error("Unexpected error processing payment-events record key={} partition={} offset={}: {}",
                    key, partition, offset, e.getMessage(), e);
        }
    }
}
