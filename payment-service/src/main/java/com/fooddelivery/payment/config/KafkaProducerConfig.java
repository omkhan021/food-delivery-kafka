package com.fooddelivery.payment.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Explicitly declares a KafkaTemplate<String, Object> bean (built from the standard
 * spring.kafka.producer.* properties in application.yml via KafkaProperties).
 *
 * Spring Boot's own auto-configuration only registers a KafkaTemplate<Object, Object> bean.
 * Because Spring's autowiring-by-type is generics-aware (and generics are invariant), a
 * constructor parameter typed KafkaTemplate<String, Object> - which is what PaymentService uses,
 * since our producer key is always the orderId String - is NOT guaranteed to match that
 * Object/Object-typed bean. Declaring our own bean with the exact generic signature we inject
 * elsewhere avoids relying on that edge case.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
