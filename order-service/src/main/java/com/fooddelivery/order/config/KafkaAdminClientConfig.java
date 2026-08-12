package com.fooddelivery.order.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Raw Kafka {@link AdminClient}, built directly from {@code spring.kafka.bootstrap-servers},
 * used by {@code KafkaAdminService} to power the {@code /api/admin/kafka/...} endpoints:
 * topic/partition introspection, consumer-group lag, and offset-reset-based replay.
 */
@Configuration
public class KafkaAdminClientConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient() {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }
}
