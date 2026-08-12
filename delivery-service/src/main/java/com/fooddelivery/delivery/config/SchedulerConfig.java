package com.fooddelivery.delivery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Shared thread pool used to run the delayed delivery-state-progression steps
 * (ASSIGNED -> PICKED_UP -> ENROUTE -> DELIVERED) without blocking the Kafka listener thread.
 *
 * <p>This is the crux of the "multi-stage delayed state machine" teaching pattern used across
 * this codebase: a {@code @KafkaListener} method must return quickly so the consumer can keep
 * polling and stay out of a rebalance. Instead of sleeping on the listener thread (which would
 * stall partition consumption for seconds at a time), we hand off each subsequent stage to this
 * scheduler with a delay, and the listener thread returns immediately after step 1.
 *
 * <p>A single small pool is enough for a demo (a handful of concurrent in-flight deliveries);
 * a production system would likely use a durable timer/workflow engine instead of in-memory
 * scheduling, since these tasks are lost on service restart.
 */
@Configuration
public class SchedulerConfig {

    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService deliveryScheduler() {
        return Executors.newScheduledThreadPool(4);
    }
}
