package com.fooddelivery.delivery.kafka;

import com.fooddelivery.delivery.config.KafkaTopicConfig;
import com.fooddelivery.delivery.dto.DeliveryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes {@link DeliveryEvent}s to {@code delivery-events}, keyed by {@code orderId} so all
 * events for a given order land on the same partition and are consumed in order.
 */
@Component
public class DeliveryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventProducer.class);

    private final KafkaTemplate<String, DeliveryEvent> kafkaTemplate;

    public DeliveryEventProducer(KafkaTemplate<String, DeliveryEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(DeliveryEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.DELIVERY_EVENTS_TOPIC, event.getOrderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish {} for order {}: {}",
                                event.getEventType(), event.getOrderId(), ex.getMessage(), ex);
                    } else {
                        log.info("Published {} for order {} (partition={}, offset={})",
                                event.getEventType(), event.getOrderId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
