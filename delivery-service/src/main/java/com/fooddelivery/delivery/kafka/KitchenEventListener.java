package com.fooddelivery.delivery.kafka;

import com.fooddelivery.delivery.dto.KitchenEvent;
import com.fooddelivery.delivery.service.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code kitchen-events}. The consumer group id ({@code delivery-service-group}) and
 * the listener {@code id} must match exactly what's registered in ARCHITECTURE.md section 3 —
 * order-service's admin/replay API looks up listener containers by this id via
 * {@code KafkaListenerEndpointRegistry}.
 *
 * <p>kitchen-service publishes 3 event types on this topic (ORDER_RECEIVED, PREPARING,
 * PREPARED) but delivery-service only cares about the last one — a driver can't be dispatched
 * until the food is actually ready. ORDER_RECEIVED/PREPARING are deliberately ignored here
 * rather than filtered out via a Kafka-level filter, so the ignore-decision and the reason for
 * it are visible in one place.
 */
@Component
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private static final String EVENT_TYPE_PREPARED = "PREPARED";

    private final DeliveryService deliveryService;

    public KitchenEventListener(DeliveryService deliveryService) {
        this.deliveryService = deliveryService;
    }

    @KafkaListener(
            id = "delivery-service-group",
            groupId = "delivery-service-group",
            topics = "kitchen-events"
    )
    public void onKitchenEvent(KitchenEvent event) {
        try {
            if (event == null) {
                log.warn("Received null KitchenEvent payload on kitchen-events - skipping");
                return;
            }

            if (event.getOrderId() == null || event.getEventType() == null) {
                log.warn("Received malformed KitchenEvent (missing orderId/eventType): {} - skipping", event);
                return;
            }

            if (!EVENT_TYPE_PREPARED.equals(event.getEventType())) {
                // ORDER_RECEIVED / PREPARING - not actionable for delivery-service, ignore.
                log.debug("Ignoring kitchen-events eventType={} for order {}", event.getEventType(), event.getOrderId());
                return;
            }

            log.info("Order {} PREPARED - starting delivery assignment", event.getOrderId());
            deliveryService.handleOrderPrepared(event.getOrderId());

        } catch (Exception ex) {
            // Never let an unexpected exception escape the listener - that would trigger the
            // container's retry/error-handling machinery for what may just be a data issue, and
            // in the worst case stall this consumer's partition assignment.
            log.error("Failed to process kitchen-events record {}: {}", event, ex.getMessage(), ex);
        }
    }
}
