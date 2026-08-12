package com.fooddelivery.payment.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A shared ScheduledExecutorService used to simulate the delay of an asynchronous payment
 * gateway call (see PaymentService).
 *
 * WHY: Kafka listener threads must never block for multi-second, unpredictable amounts of time.
 * A @KafkaListener container has a fixed, small pool of consumer threads; if one of them
 * sleeps/blocks inside the listener method, that thread stops polling Kafka. If it blocks for
 * longer than max.poll.interval.ms, Kafka's group coordinator assumes the consumer has died and
 * triggers a rebalance, which can cause duplicate processing and cascading latency across the
 * whole consumer group. So instead of doing Thread.sleep(processingDelayMs) inside the listener,
 * we persist the PENDING payment, publish PAYMENT_PROCESSING immediately, and hand off the
 * "wait, then decide success/failure" work to this separate executor. The listener thread returns
 * right away and goes back to polling Kafka; the delayed outcome runs later on a scheduler thread.
 */
@Configuration
public class SchedulerConfig {

    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService paymentScheduledExecutor() {
        ThreadFactory namedThreads = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "payment-gateway-sim-" + counter.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        };
        return Executors.newScheduledThreadPool(4, namedThreads);
    }
}
