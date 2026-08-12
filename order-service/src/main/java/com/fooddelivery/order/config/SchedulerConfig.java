package com.fooddelivery.order.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A single shared {@link ScheduledExecutorService} used to schedule the delayed
 * DELIVERED -&gt; COMPLETED order transition (see {@code KafkaEventConsumerService}) without
 * blocking the Kafka listener container thread. Using {@code Thread.sleep} or blocking directly
 * inside a {@code @KafkaListener} method would stall partition consumption/poll heartbeats, so
 * the 2s delay is offloaded to this executor instead.
 */
@Configuration
public class SchedulerConfig {

    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService scheduledExecutorService() {
        return Executors.newScheduledThreadPool(2);
    }
}
