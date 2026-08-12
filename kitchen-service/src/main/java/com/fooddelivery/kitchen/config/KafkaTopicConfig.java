package com.fooddelivery.kitchen.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topic this service produces to: {@code kitchen-events}.
 *
 * <p>Registering a {@link NewTopic} bean lets Spring's {@code KafkaAdmin} auto-create the
 * topic on startup if it doesn't already exist. This is idempotent/safe to run from every
 * service instance and every service in the demo: {@code KafkaAdmin} treats
 * {@code TopicExistsException} from the broker as a no-op, so whichever service happens to
 * start first creates the topic and every later startup (by this service or any other) is
 * a harmless "already exists" check - equivalent in effect to "create if not exists".
 */
@Configuration
public class KafkaTopicConfig {

    public static final String KITCHEN_EVENTS_TOPIC = "kitchen-events";

    @Bean
    public NewTopic kitchenEventsTopic() {
        return TopicBuilder.name(KITCHEN_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
