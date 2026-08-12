package com.fooddelivery.notification.config;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer-side Kafka wiring for the single fan-in listener.
 *
 * <h2>Why one consumer group can safely subscribe to 4 topics</h2>
 * A Kafka consumer group is just a named set of consumers that share the work of reading
 * one or more topics. There is nothing in Kafka that ties a group to a single topic —
 * a {@code @KafkaListener} can list several topic names and the group will get its own
 * independent set of partition assignments and committed offsets *per topic*. Since
 * notification-service does not need per-topic ordering guarantees relative to each
 * other (it only cares about writing every event it sees, once, in roughly received
 * order), one listener/group reading all 4 topics is simpler and cheaper than running 4
 * separate listener containers. Each of the other services intentionally uses its own
 * dedicated group per upstream topic (see ARCHITECTURE.md section 3) because *they* only
 * ever care about one topic each; notification-service is the exception because its job
 * is explicitly "see everything".
 *
 * <h2>Why {@link ErrorHandlingDeserializer}</h2>
 * If a message on any topic is malformed (not valid JSON, unexpected shape, etc.), a
 * plain {@code JsonDeserializer} throws during poll() and kills the consumer thread
 * before the listener method ever runs — there is no way to catch that in application
 * code. Wrapping it in {@code ErrorHandlingDeserializer} converts a deserialization
 * failure into a {@code DeserializationException} that is handed to the container's
 * error handler instead, so one poison-pill message logs and gets skipped rather than
 * crashing the entire fan-in listener.
 */
@Configuration
@EnableKafka
@Slf4j
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, JsonNode> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-service-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Delegate (de)serializers wrapped by ErrorHandlingDeserializer — see class javadoc.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // Generic deserialization: the 4 topics carry different JSON shapes and this
        // service intentionally does not share DTO classes with the producer services
        // (each service is an independently deployable unit per ARCHITECTURE.md section
        // 3), so we deserialize to Jackson's JsonNode and pull out fields by name.
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, JsonNode.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, JsonNode> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, JsonNode> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // A malformed/poison-pill record (or an uncaught exception thrown from the
        // listener method itself) is logged and the offset is committed anyway after 2
        // quick retries, so the listener thread keeps consuming the other 3 topics
        // instead of getting stuck retrying the same bad record forever.
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> log.error(
                        "Skipping unprocessable record on topic={} partition={} offset={}: {}",
                        record.topic(), record.partition(), record.offset(), exception.getMessage()),
                new FixedBackOff(500L, 2)
        );
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
