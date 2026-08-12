package com.fooddelivery.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares all 4 project-wide Kafka topics as {@link NewTopic} beans so they auto-create with the
 * agreed partition count/replication factor regardless of which service happens to start first.
 *
 * <p>order-service only <em>produces</em> to {@code order-events}, but per ARCHITECTURE.md
 * section 3 it's good practice for every service to declare all 4 topics here (idempotent,
 * {@code ifNotExists}) since in a fresh docker-compose environment this service is commonly the
 * first one up. Declaring a topic that already exists (created by another service) is a no-op.
 */
@Configuration
public class KafkaTopicConfig {

    private static final int PARTITIONS = 3;
    private static final int REPLICATION_FACTOR = 1;

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name("order-events")
                .partitions(PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                .build();
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name("payment-events")
                .partitions(PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                .build();
    }

    @Bean
    public NewTopic kitchenEventsTopic() {
        return TopicBuilder.name("kitchen-events")
                .partitions(PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                .build();
    }

    @Bean
    public NewTopic deliveryEventsTopic() {
        return TopicBuilder.name("delivery-events")
                .partitions(PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                .build();
    }
}
