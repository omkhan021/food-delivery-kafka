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

/**
 * Manual-mode kitchen state machine.
 *
 * When PAYMENT_COMPLETED arrives, the kitchen immediately acknowledges the order
 * (persists RECEIVED + publishes ORDER_RECEIVED) and then waits for manual REST
 * triggers from the frontend for each subsequent stage:
 *
 *   PAYMENT_COMPLETED (Kafka) → auto: ORDER_RECEIVED
 *   POST /api/kitchen/{id}/prepare → manual: PREPARING
 *   POST /api/kitchen/{id}/ready  → manual: PREPARED
 */
@Service
public class KitchenOrderService {

    private static final Logger log = LoggerFactory.getLogger(KitchenOrderService.class);

    private final KitchenOrderRepository repository;
    private final KitchenEventProducer producer;
    private final KitchenProperties properties;
    private final Random random = new Random();

    public KitchenOrderService(KitchenOrderRepository repository,
                               KitchenEventProducer producer,
                               KitchenProperties properties) {
        this.repository = repository;
        this.producer = producer;
        this.properties = properties;
    }

    /**
     * Called by the Kafka listener for every PAYMENT_COMPLETED event.
     * Auto-acknowledges (ORDER_RECEIVED) and stops — next steps are manual.
     */
    public void handlePaymentCompleted(String orderId) {
        if (repository.findByOrderId(orderId).isPresent()) {
            log.info("kitchen_orders row already exists for orderId={} — ignoring duplicate PAYMENT_COMPLETED", orderId);
            return;
        }

        KitchenOrder order = new KitchenOrder(orderId, KitchenOrderStatus.RECEIVED, Instant.now());
        repository.save(order);
        log.info("Order {} acknowledged by kitchen (RECEIVED)", orderId);

        producer.publish(KitchenEvent.orderReceived(orderId));
        // No auto-scheduling — waits for manual /prepare trigger
    }

    /** Manual trigger: RECEIVED → PREPARING. Publishes PREPARING event. */
    public void manualPrepare(String orderId) {
        KitchenOrder order = repository.findByOrderId(orderId)
                .orElseThrow(() -> new IllegalStateException("No kitchen order for orderId=" + orderId));

        if (order.getStatus() != KitchenOrderStatus.RECEIVED) {
            throw new IllegalStateException(
                    "Order " + orderId + " is in state " + order.getStatus() + ", expected RECEIVED");
        }

        int estimatedMinutes = randomEstimatedMinutes();
        order.setStatus(KitchenOrderStatus.PREPARING);
        order.setEstimatedMinutes(estimatedMinutes);
        order.setPreparingAt(Instant.now());
        repository.save(order);

        log.info("Order {} manually set to PREPARING (eta {} min)", orderId, estimatedMinutes);
        producer.publish(KitchenEvent.preparing(orderId, estimatedMinutes));
    }

    /** Manual trigger: PREPARING → PREPARED. Publishes PREPARED event. */
    public void manualReady(String orderId) {
        KitchenOrder order = repository.findByOrderId(orderId)
                .orElseThrow(() -> new IllegalStateException("No kitchen order for orderId=" + orderId));

        if (order.getStatus() != KitchenOrderStatus.PREPARING) {
            throw new IllegalStateException(
                    "Order " + orderId + " is in state " + order.getStatus() + ", expected PREPARING");
        }

        order.setStatus(KitchenOrderStatus.PREPARED);
        order.setPreparedAt(Instant.now());
        repository.save(order);

        log.info("Order {} manually set to PREPARED", orderId);
        producer.publish(KitchenEvent.prepared(orderId));
    }

    private int randomEstimatedMinutes() {
        int min = properties.getMinPrepMinutes();
        int max = properties.getMaxPrepMinutes();
        return (max <= min) ? min : min + random.nextInt(max - min + 1);
    }
}
