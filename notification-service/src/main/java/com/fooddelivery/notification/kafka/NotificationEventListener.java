package com.fooddelivery.notification.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fooddelivery.notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The single fan-in consumer for the whole demo: one listener, one consumer group
 * ("notification-service-group"), subscribed to all 4 event topics at once.
 *
 * <p>The {@code id} attribute is set to the same string as the consumer group id
 * because ARCHITECTURE.md section 3 requires {@code @KafkaListener} ids to match their
 * group id exactly — order-service's replay API looks up listener containers by this id
 * via {@code KafkaListenerEndpointRegistry}.
 *
 * <p>Payloads are deserialized generically to Jackson's {@link JsonNode} (configured in
 * {@link com.fooddelivery.notification.config.KafkaConsumerConfig}) rather than to a
 * typed DTO, because the 4 topics carry 4 different JSON shapes (OrderEvent,
 * PaymentEvent, KitchenEvent, DeliveryEvent) and this service does not own or share
 * those DTO classes with the producing services. All 4 shapes do share
 * {@code eventType}/{@code orderId}/{@code timestamp}, which is all this service needs.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationEventListener {

    private final NotificationService notificationService;

    @KafkaListener(
            id = "notification-service-group",
            groupId = "notification-service-group",
            topics = {"order-events", "payment-events", "kitchen-events", "delivery-events"}
    )
    public void onEvent(ConsumerRecord<String, JsonNode> record) {
        String topic = record.topic();
        JsonNode payload = record.value();

        try {
            if (payload == null) {
                log.warn("Received null/empty payload on topic {} (key={}, offset={}), skipping",
                        topic, record.key(), record.offset());
                return;
            }
            notificationService.handleEvent(topic, payload);
        } catch (Exception ex) {
            // Belt-and-braces: a single malformed or unexpected event must never take
            // down the shared fan-in listener that all 4 topics depend on. Deserialization
            // failures are already handled by ErrorHandlingDeserializer + the container's
            // error handler (KafkaConsumerConfig); this catches everything else that could
            // go wrong while building the message or persisting the row (e.g. an
            // unexpected null field, a DB hiccup for one record).
            log.error("Failed to process event on topic {} (key={}, offset={}): {}",
                    topic, record.key(), record.offset(), ex.getMessage(), ex);
        }
    }
}
