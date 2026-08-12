package com.fooddelivery.kitchen.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Registers a {@link CommonErrorHandler} that Spring Boot's auto-configured
 * {@code ConcurrentKafkaListenerContainerFactory} will pick up automatically.
 *
 * <p>Two distinct kinds of "bad message" are handled defensively so a single poison
 * record on {@code payment-events} can never crash or wedge the {@code kitchen-service-group}
 * consumer:
 * <ol>
 *   <li><b>Undeserializable bytes</b> (not valid JSON, or JSON that doesn't fit the target
 *       type at all): caught by the {@code ErrorHandlingDeserializer} configured in
 *       application.yml, which delegates the failure here instead of throwing out of
 *       {@code poll()}.</li>
 *   <li><b>Deserializes fine but has an unexpected/irrelevant {@code eventType}</b>
 *       (e.g. {@code PAYMENT_PROCESSING}, {@code PAYMENT_FAILED}) or otherwise fails
 *       business logic: handled inside the listener method itself with a try/catch
 *       (see {@code PaymentEventListener}), which is a *processing* concern, not a
 *       deserialization concern.</li>
 * </ol>
 *
 * <p>We use a {@link FixedBackOff} of zero retries: for a demo, a malformed record is
 * logged and skipped immediately (no point retrying a record that will never parse
 * differently) rather than blocking the partition while retrying.
 */
@Configuration
public class KafkaConsumerErrorConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerErrorConfig.class);

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(
                (record, exception) -> log.error(
                        "Skipping unprocessable Kafka record on topic={} partition={} offset={} key={}: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), exception.getMessage(), exception),
                new FixedBackOff(0L, 0L));
    }
}
