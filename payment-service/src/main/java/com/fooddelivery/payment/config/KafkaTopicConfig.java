package com.fooddelivery.payment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topic payment-service produces to. Spring Kafka's auto-configured KafkaAdmin
 * picks up NewTopic beans and calls AdminClient.createTopics() on startup; if the topic already
 * exists (e.g. another service instance created it first) the admin simply leaves it alone and
 * logs it - this is what makes topic creation idempotent ("ifNotExists") across the 6 services
 * that all start up independently and race to create their topics.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name("payment-events")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
