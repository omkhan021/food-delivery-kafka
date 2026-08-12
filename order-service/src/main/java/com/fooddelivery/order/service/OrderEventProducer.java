package com.fooddelivery.order.service;

import com.fooddelivery.order.dto.events.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes to {@code order-events}, the one topic order-service produces to. Key = orderId,
 * guaranteeing every event for a given order lands on the same partition and is therefore
 * strictly ordered for consumers (ARCHITECTURE.md section 3).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventProducer {

    private static final String TOPIC = "order-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publish(OrderEvent event) {
        kafkaTemplate.send(TOPIC, event.getOrderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish {} for order {} to {}: {}",
                                event.getEventType(), event.getOrderId(), TOPIC, ex.getMessage(), ex);
                    } else {
                        log.info("Published {} for order {} to {}-{}@{}",
                                event.getEventType(), event.getOrderId(), TOPIC,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
