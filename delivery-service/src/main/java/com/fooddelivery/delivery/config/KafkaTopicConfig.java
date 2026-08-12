package com.fooddelivery.delivery.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topic this service owns/produces to. Spring Kafka's {@link NewTopic} beans are
 * picked up by a {@code KafkaAdmin} at startup and created idempotently ({@code ifNotExists}) —
 * safe even if multiple services race to create the same topic, and safe to re-run on every
 * boot.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String DELIVERY_EVENTS_TOPIC = "delivery-events";

    @Bean
    public NewTopic deliveryEventsTopic() {
        return TopicBuilder.name(DELIVERY_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
