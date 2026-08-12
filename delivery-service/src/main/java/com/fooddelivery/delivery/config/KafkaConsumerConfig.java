package com.fooddelivery.delivery.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Error handling for the {@code delivery-service-group} consumer.
 *
 * <p>Two independent layers protect the listener thread from bad data on {@code kitchen-events}:
 * <ol>
 *   <li>{@link ErrorHandlingDeserializer} (configured in application.yml) catches JSON payloads
 *       that can't even be deserialized into {@code KitchenEvent} (a "poison pill") and hands the
 *       resulting exception to the container's error handler instead of throwing out of the
 *       consumer poll loop.</li>
 *   <li>This {@link CommonErrorHandler} decides what to do once an error reaches it: log it and
 *       move on. We use a {@link FixedBackOff#ZERO} (no retries) because a deserialization
 *       failure is never going to succeed on retry — the bytes on the topic don't change.</li>
 * </ol>
 * Application-level problems (e.g. an unrecognized {@code eventType}) are handled separately,
 * inside the listener itself, by simply ignoring the event — see {@code KitchenEventListener}.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (ConsumerRecord<?, ?> record, Exception exception) ->
                        log.error("Skipping unprocessable record on topic={} partition={} offset={} key={}: {}",
                                record.topic(), record.partition(), record.offset(), record.key(), exception.getMessage()),
                new FixedBackOff(0L, 0L));

        // Deserialization exceptions are never transient - don't bother retrying them.
        handler.addNotRetryableExceptions(org.apache.kafka.common.errors.SerializationException.class);
        return handler;
    }
}
