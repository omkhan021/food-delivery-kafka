package com.fooddelivery.kitchen.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Explicitly declares a {@code KafkaTemplate<String, Object>} / {@code ProducerFactory<String, Object>}
 * pair, built from the {@code spring.kafka.producer.*} properties in application.yml
 * (bootstrap-servers, String key serializer, JSON value serializer,
 * {@code spring.json.add.type.headers=false}).
 *
 * <p>We do this instead of relying on Spring Boot's auto-configured
 * {@code KafkaTemplate<Object, Object>} bean purely to keep the injected type in
 * {@code KitchenEventProducer} (and any test code) precisely
 * {@code KafkaTemplate<String, Object>} without depending on generic-type autowiring
 * edge cases - this bean simply takes over from (and disables, via
 * {@code @ConditionalOnMissingBean} on Boot's side) the auto-configured one.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> kitchenProducerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(null));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> kitchenProducerFactory) {
        return new KafkaTemplate<>(kitchenProducerFactory);
    }
}
