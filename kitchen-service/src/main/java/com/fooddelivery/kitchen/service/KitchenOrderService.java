package com.fooddelivery.kitchen.service;

import com.fooddelivery.kitchen.config.KitchenProperties;
import com.fooddelivery.kitchen.domain.KitchenOrder;
import com.fooddelivery.kitchen.domain.KitchenOrderStatus;
import com.fooddelivery.kitchen.dto.KitchenEvent;
import com.fooddelivery.kitchen.kafka.KitchenEventProducer;
import com.fooddelivery.kitchen.repository.KitchenOrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Owns the kitchen's delayed, three-stage state machine:
 *
 * <pre>
 *   PAYMENT_COMPLETED (Kafka)
 *          |
 *          v
 *   [1] RECEIVED   --persist + publish ORDER_RECEIVED-- (synchronous, on the Kafka listener thread)
 *          |  wait kitchen.prep-start-delay-ms  (off-thread, via ScheduledExecutorService)
 *          v
 *   [2] PREPARING  --persist + publish PREPARING(estimatedMinutes)--
 *          |  wait kitchen.prep-duration-ms     (off-thread, via ScheduledExecutorService)
 *          v
 *   [3] PREPARED   --persist + publish PREPARED--
 * </pre>
 *
 * <p><b>Why this pattern, and why it's worth studying:</b> this simulates a long-running
 * kitchen prep process without actually blocking anything for real minutes, and - more
 * importantly for a Kafka teaching demo - it shows how a consumer can react to one event
 * by producing a *sequence* of downstream events over time, entirely decoupled from the
 * original consumer poll/commit cycle. Step [1] happens inline (fast, synchronous) so the
 * Kafka offset for the triggering PAYMENT_COMPLETED record can be committed promptly.
 * Steps [2] and [3] are scheduled onto {@link ScheduledExecutorService} (see
 * {@code SchedulerConfig}) so the Kafka consumer thread is never blocked waiting on them.
 *
 * <p><b>Trade-off, called out honestly:</b> because the pending "wait and then transition"
 * work lives only in the JVM's scheduled executor (not persisted anywhere), if this service
 * restarts between steps, any in-flight order will get stuck in RECEIVED or PREPARING
 * forever - there is no recovery/resume-on-startup logic. That's an acceptable, explicit
 * simplification for a demo app; a production saga would persist the "next step due at"
 * time and use a durable scheduler (e.g. a polling job, Quartz, or a delay topic) instead.
 */
@Service
public class KitchenOrderService {

    private static final Logger log = LoggerFactory.getLogger(KitchenOrderService.class);

    private final KitchenOrderRepository repository;
    private final KitchenEventProducer producer;
    private final ScheduledExecutorService scheduler;
    private final KitchenProperties properties;
    private final Random random = new Random();

    public KitchenOrderService(KitchenOrderRepository repository,
                                KitchenEventProducer producer,
                                ScheduledExecutorService kitchenScheduler,
                                KitchenProperties properties) {
        this.repository = repository;
        this.producer = producer;
        this.scheduler = kitchenScheduler;
        this.properties = properties;
    }

    /**
     * Entry point called synchronously from {@code PaymentEventListener} for every
     * PAYMENT_COMPLETED record. Persists the RECEIVED row, publishes ORDER_RECEIVED,
     * then schedules the PREPARING transition and returns immediately.
     *
     * <p>Note: each {@link KitchenOrderRepository} call (findByOrderId / save) runs in
     * its own Spring Data-managed transaction (Spring Boot has {@code open-in-view}
     * disabled here and {@code SimpleJpaRepository} methods are individually
     * transactional), so no explicit {@code @Transactional} is needed on this class -
     * that also sidesteps the classic Spring self-invocation pitfall that would occur
     * if a proxied {@code @Transactional} method here called another proxied method on
     * {@code this} from inside a scheduled lambda.
     */
    public void handlePaymentCompleted(String orderId) {
        if (repository.findByOrderId(orderId).isPresent()) {
            // Kafka delivery is at-least-once. A redelivered PAYMENT_COMPLETED (e.g. after
            // a consumer restart before the offset commit, or a manual replay via the
            // admin API) must not restart the state machine for an order we've already
            // seen - that would double-publish kitchen-events for the same order.
            log.info("kitchen_orders row already exists for orderId={}, ignoring duplicate PAYMENT_COMPLETED", orderId);
            return;
        }

        KitchenOrder order = new KitchenOrder(orderId, KitchenOrderStatus.RECEIVED, Instant.now());
        repository.save(order);
        log.info("Order {} RECEIVED by kitchen", orderId);

        producer.publish(KitchenEvent.orderReceived(orderId));

        scheduler.schedule(
                () -> transitionToPreparing(orderId),
                properties.getPrepStartDelayMs(),
                TimeUnit.MILLISECONDS);
    }

    /** Stage 2: RECEIVED -> PREPARING. Runs on a kitchenScheduler thread, not the Kafka listener thread. */
    private void transitionToPreparing(String orderId) {
        try {
            KitchenOrder order = repository.findByOrderId(orderId).orElse(null);
            if (order == null) {
                log.warn("No kitchen_orders row found for orderId={} when transitioning to PREPARING (skipping)", orderId);
                return;
            }

            int estimatedMinutes = randomEstimatedMinutes();
            order.setStatus(KitchenOrderStatus.PREPARING);
            order.setEstimatedMinutes(estimatedMinutes);
            order.setPreparingAt(Instant.now());
            repository.save(order);
            log.info("Order {} PREPARING, estimatedMinutes={}", orderId, estimatedMinutes);

            producer.publish(KitchenEvent.preparing(orderId, estimatedMinutes));

            // Schedule stage 3 relative to *this* step completing, per the spec
            // ("after kitchen.prep-duration-ms from the PREPARING step").
            scheduler.schedule(
                    () -> transitionToPrepared(orderId),
                    properties.getPrepDurationMs(),
                    TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // A scheduled task runs outside the Kafka listener's error handling entirely,
            // so it needs its own defensive catch - an uncaught exception here would just
            // silently die on the executor thread with nothing to retry it.
            log.error("Failed PREPARING transition for orderId={}: {}", orderId, e.getMessage(), e);
        }
    }

    /** Stage 3: PREPARING -> PREPARED (terminal state for this service). */
    private void transitionToPrepared(String orderId) {
        try {
            KitchenOrder order = repository.findByOrderId(orderId).orElse(null);
            if (order == null) {
                log.warn("No kitchen_orders row found for orderId={} when transitioning to PREPARED (skipping)", orderId);
                return;
            }

            order.setStatus(KitchenOrderStatus.PREPARED);
            order.setPreparedAt(Instant.now());
            repository.save(order);
            log.info("Order {} PREPARED", orderId);

            producer.publish(KitchenEvent.prepared(orderId));
        } catch (Exception e) {
            log.error("Failed PREPARED transition for orderId={}: {}", orderId, e.getMessage(), e);
        }
    }

    private int randomEstimatedMinutes() {
        int min = properties.getMinPrepMinutes();
        int max = properties.getMaxPrepMinutes();
        if (max <= min) {
            return min;
        }
        return min + random.nextInt(max - min + 1);
    }
}
