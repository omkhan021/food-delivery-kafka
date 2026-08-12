package com.fooddelivery.kitchen.kafka;

import com.fooddelivery.kitchen.config.KafkaTopicConfig;
import com.fooddelivery.kitchen.dto.KitchenEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Thin wrapper around {@link KafkaTemplate} for publishing to {@code kitchen-events}.
 *
 * <p>The message key is always the {@code orderId}: per ARCHITECTURE.md section 3, all
 * 4 topics key by {@code orderId} so that every event for a given order lands on the
 * same partition and is therefore strictly ordered (ORDER_RECEIVED before PREPARING
 * before PREPARED can never be observed out of order by a consumer of this topic).
 */
@Component
public class KitchenEventProducer {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KitchenEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(KitchenEvent event) {
        log.info("Publishing {} for orderId={} to topic={}", event.getEventType(), event.getOrderId(), KafkaTopicConfig.KITCHEN_EVENTS_TOPIC);
        kafkaTemplate.send(KafkaTopicConfig.KITCHEN_EVENTS_TOPIC, event.getOrderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish {} for orderId={}: {}", event.getEventType(), event.getOrderId(), ex.getMessage(), ex);
                    }
                });
    }
}
