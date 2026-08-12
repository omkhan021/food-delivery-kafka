package com.fooddelivery.kitchen.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides the single shared {@link ScheduledExecutorService} used to drive the
 * kitchen's delayed, multi-stage state machine (RECEIVED -> PREPARING -> PREPARED).
 *
 * <p><b>Why not just block inside the {@code @KafkaListener} method?</b> Spring Kafka
 * invokes listener methods on the container's consumer thread(s). Calling
 * {@code Thread.sleep(...)} there (or any blocking wait) would stall that thread,
 * delay committing offsets, and - because there's typically one consumer thread
 * per partition - block processing of every other message in that partition
 * (and, at the extreme, trigger a consumer group rebalance if the poll loop
 * stalls past {@code max.poll.interval.ms}). Kafka listener threads must return quickly.
 *
 * <p>Instead, the listener does its synchronous work (persist + publish ORDER_RECEIVED)
 * and then hands off the "wait N ms, then do the next step" work to this executor,
 * which runs on its own pool of daemon-like threads completely decoupled from the
 * Kafka consumer. This is a common, simple way to teach/demonstrate a saga-style
 * delayed state machine without pulling in a full workflow engine.
 */
@Configuration
public class SchedulerConfig {

    /**
     * A small fixed-size pool is plenty here: each order only ever has at most one
     * pending scheduled task at a time (PREPARING, then later PREPARED), and this is
     * a demo with a handful of concurrent orders, not a production-scale system.
     */
    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService kitchenScheduler() {
        return Executors.newScheduledThreadPool(4);
    }
}
