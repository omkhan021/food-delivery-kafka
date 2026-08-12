package com.fooddelivery.payment.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka error handling.
 *
 * application.yml wraps the real JsonDeserializer in an ErrorHandlingDeserializer, which turns a
 * deserialization failure (malformed JSON, a payload that doesn't match OrderEvent, etc. - a
 * "poison pill") into a DeserializationException delivered to the container's error handler
 * instead of crashing the listener thread.
 *
 * This DefaultErrorHandler is picked up automatically by Spring Boot's auto-configured
 * ConcurrentKafkaListenerContainerFactory. FixedBackOff(0, 0) means "don't retry" - a poison pill
 * will never succeed no matter how many times we retry deserializing it, so we log it and move on
 * to the next record immediately. Errors thrown *inside* the @KafkaListener method itself (e.g. a
 * transient DB problem) are also routed here if the listener method doesn't already catch them,
 * though OrderEventListener catches its own exceptions so the consumer keeps running either way.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, exception) -> log.error(
                        "Skipping unprocessable Kafka record: topic={}, partition={}, offset={}, key={}, error={}",
                        record.topic(), record.partition(), record.offset(), record.key(), exception.getMessage()),
                new FixedBackOff(0L, 0L));
        return handler;
    }
}
