package com.fooddelivery.delivery.service;

import com.fooddelivery.delivery.dto.DeliveryEvent;
import com.fooddelivery.delivery.dto.Driver;
import com.fooddelivery.delivery.entity.Delivery;
import com.fooddelivery.delivery.entity.DeliveryStatus;
import com.fooddelivery.delivery.kafka.DeliveryEventProducer;
import com.fooddelivery.delivery.repository.DeliveryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Core business logic for the delivery leg of the saga.
 *
 * <h2>The multi-stage delayed state machine pattern</h2>
 * A real delivery takes minutes; this demo compresses it into a few seconds so the whole
 * end-to-end saga is visible quickly in the UI, but the *shape* of the logic mirrors a real
 * system: a sequence of state transitions, each triggered some time after the previous one.
 *
 * <p>Kafka listener threads must never block for the duration of a business process — doing so
 * would stall partition consumption (and eventually trigger a consumer-group rebalance if the
 * poll loop stalls past {@code max.poll.interval.ms}). So instead of sleeping through
 * ASSIGNED -> PICKED_UP -> ENROUTE -> DELIVERED synchronously, each stage is scheduled on a
 * shared {@link ScheduledExecutorService} bean ({@code deliveryScheduler}) with a delay, and
 * schedules the *next* stage itself once it runs. The listener thread that started this chain
 * returns immediately after persisting the row and publishing DRIVER_ASSIGNED.
 *
 * <p>This is a lightweight substitute for a durable saga/workflow engine (e.g. Temporal): it's
 * simple and great for teaching the concept, but the in-flight timers are lost if the service
 * restarts mid-delivery — acceptable for a demo, not for production.
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final DeliveryRepository deliveryRepository;
    private final DeliveryEventProducer eventProducer;
    private final DriverRoster driverRoster;
    private final ScheduledExecutorService deliveryScheduler;

    @Value("${delivery.pickup-delay-ms:3000}")
    private long pickupDelayMs;

    @Value("${delivery.enroute-delay-ms:3000}")
    private long enrouteDelayMs;

    @Value("${delivery.delivered-delay-ms:5000}")
    private long deliveredDelayMs;

    @Value("${delivery.min-eta-minutes:15}")
    private int minEtaMinutes;

    @Value("${delivery.max-eta-minutes:35}")
    private int maxEtaMinutes;

    public DeliveryService(DeliveryRepository deliveryRepository,
                            DeliveryEventProducer eventProducer,
                            DriverRoster driverRoster,
                            ScheduledExecutorService deliveryScheduler) {
        this.deliveryRepository = deliveryRepository;
        this.eventProducer = eventProducer;
        this.driverRoster = driverRoster;
        this.deliveryScheduler = deliveryScheduler;
    }

    /**
     * Entry point invoked by the Kafka listener when a {@code PREPARED} event arrives on
     * {@code kitchen-events}. Runs on the listener thread but only does a quick DB write + async
     * publish before returning — the rest of the state machine runs on the scheduler.
     */
    @Transactional
    public void handleOrderPrepared(String orderId) {
        Driver driver = driverRoster.pickRandomDriver();

        Delivery delivery = new Delivery();
        delivery.setOrderId(orderId);
        delivery.setDriverId(driver.id());
        delivery.setDriverName(driver.name());
        delivery.setStatus(DeliveryStatus.ASSIGNED);
        delivery.setAssignedAt(Instant.now());
        delivery = deliveryRepository.save(delivery);

        log.info("Order {} prepared -> assigned driver {} ({})", orderId, driver.id(), driver.name());

        eventProducer.publish(new DeliveryEvent(
                UUID.randomUUID().toString(),
                "DRIVER_ASSIGNED",
                orderId,
                Instant.now(),
                driver.id(),
                driver.name(),
                null
        ));

        scheduleNextStage(delivery.getId(), orderId, pickupDelayMs, this::advanceToPickedUp);
    }

    /** Stage 2: ASSIGNED -> PICKED_UP, then schedules stage 3. */
    private void advanceToPickedUp(Long deliveryId, String orderId) {
        deliveryRepository.findById(deliveryId).ifPresentOrElse(delivery -> {
            delivery.setStatus(DeliveryStatus.PICKED_UP);
            delivery.setPickedUpAt(Instant.now());
            deliveryRepository.save(delivery);

            log.info("Order {} -> PICKED_UP", orderId);
            eventProducer.publish(new DeliveryEvent(
                    UUID.randomUUID().toString(), "PICKED_UP", orderId, Instant.now(),
                    null, null, null
            ));

            scheduleNextStage(deliveryId, orderId, enrouteDelayMs, this::advanceToEnroute);
        }, () -> log.warn("Delivery {} for order {} not found when advancing to PICKED_UP", deliveryId, orderId));
    }

    /** Stage 3: PICKED_UP -> ENROUTE (with a random ETA), then schedules stage 4. */
    private void advanceToEnroute(Long deliveryId, String orderId) {
        deliveryRepository.findById(deliveryId).ifPresentOrElse(delivery -> {
            int etaMinutes = ThreadLocalRandom.current().nextInt(minEtaMinutes, maxEtaMinutes + 1);

            delivery.setStatus(DeliveryStatus.ENROUTE);
            delivery.setEnrouteAt(Instant.now());
            delivery.setEtaMinutes(etaMinutes);
            deliveryRepository.save(delivery);

            log.info("Order {} -> ENROUTE (eta {} min)", orderId, etaMinutes);
            eventProducer.publish(new DeliveryEvent(
                    UUID.randomUUID().toString(), "ENROUTE", orderId, Instant.now(),
                    null, null, etaMinutes
            ));

            scheduleNextStage(deliveryId, orderId, deliveredDelayMs, this::advanceToDelivered);
        }, () -> log.warn("Delivery {} for order {} not found when advancing to ENROUTE", deliveryId, orderId));
    }

    /** Stage 4 (terminal): ENROUTE -> DELIVERED. */
    private void advanceToDelivered(Long deliveryId, String orderId) {
        deliveryRepository.findById(deliveryId).ifPresentOrElse(delivery -> {
            delivery.setStatus(DeliveryStatus.DELIVERED);
            delivery.setDeliveredAt(Instant.now());
            deliveryRepository.save(delivery);

            log.info("Order {} -> DELIVERED", orderId);
            eventProducer.publish(new DeliveryEvent(
                    UUID.randomUUID().toString(), "DELIVERED", orderId, Instant.now(),
                    null, null, null
            ));
        }, () -> log.warn("Delivery {} for order {} not found when advancing to DELIVERED", deliveryId, orderId));
    }

    /**
     * Schedules {@code stage} to run after {@code delayMs} on the shared scheduler. Any
     * exception thrown by a stage is caught and logged rather than propagated — an uncaught
     * exception inside a {@link ScheduledExecutorService} task silently kills that task (and
     * with some executor configurations can suppress future scheduling), so this is the async
     * equivalent of the try/catch that protects the Kafka listener thread.
     */
    private void scheduleNextStage(Long deliveryId, String orderId, long delayMs, StageAction action) {
        deliveryScheduler.schedule(() -> {
            try {
                action.run(deliveryId, orderId);
            } catch (Exception ex) {
                log.error("Delivery state machine step failed for order {} (deliveryId={}): {}",
                        orderId, deliveryId, ex.getMessage(), ex);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    @FunctionalInterface
    private interface StageAction {
        void run(Long deliveryId, String orderId);
    }

    @Transactional(readOnly = true)
    public List<Delivery> findAllNewestFirst() {
        return deliveryRepository.findAllByOrderByIdDesc();
    }

    @Transactional(readOnly = true)
    public List<Delivery> findByOrderId(String orderId) {
        return deliveryRepository.findByOrderIdOrderByIdDesc(orderId);
    }
}
