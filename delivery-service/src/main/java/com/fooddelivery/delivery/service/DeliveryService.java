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
import java.util.concurrent.ThreadLocalRandom;

/**
 * Manual-mode delivery state machine.
 *
 * When PREPARED arrives from kitchen-events, a driver is auto-assigned immediately
 * (DRIVER_ASSIGNED) and we stop. Every subsequent stage is triggered manually via REST:
 *
 *   PREPARED (Kafka) → auto: DRIVER_ASSIGNED
 *   POST /api/deliveries/{id}/pickup  → manual: PICKED_UP
 *   POST /api/deliveries/{id}/enroute → manual: ENROUTE
 *   POST /api/deliveries/{id}/deliver → manual: DELIVERED
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final DeliveryRepository deliveryRepository;
    private final DeliveryEventProducer eventProducer;
    private final DriverRoster driverRoster;

    @Value("${delivery.min-eta-minutes:15}")
    private int minEtaMinutes;

    @Value("${delivery.max-eta-minutes:35}")
    private int maxEtaMinutes;

    public DeliveryService(DeliveryRepository deliveryRepository,
                           DeliveryEventProducer eventProducer,
                           DriverRoster driverRoster) {
        this.deliveryRepository = deliveryRepository;
        this.eventProducer = eventProducer;
        this.driverRoster = driverRoster;
    }

    /**
     * Called by the Kafka listener when PREPARED arrives.
     * Auto-assigns a driver and publishes DRIVER_ASSIGNED, then stops.
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
        deliveryRepository.save(delivery);

        log.info("Order {} prepared → driver {} ({}) assigned", orderId, driver.id(), driver.name());

        eventProducer.publish(new DeliveryEvent(
                UUID.randomUUID().toString(), "DRIVER_ASSIGNED", orderId, Instant.now(),
                driver.id(), driver.name(), null
        ));
        // No auto-scheduling — next steps are manual
    }

    /** Manual trigger: ASSIGNED → PICKED_UP. Publishes PICKED_UP to delivery-events. */
    @Transactional
    public void manualPickup(String orderId) {
        Delivery delivery = findLatestByOrderId(orderId);
        delivery.setStatus(DeliveryStatus.PICKED_UP);
        delivery.setPickedUpAt(Instant.now());
        deliveryRepository.save(delivery);

        log.info("Order {} manually set to PICKED_UP", orderId);
        eventProducer.publish(new DeliveryEvent(
                UUID.randomUUID().toString(), "PICKED_UP", orderId, Instant.now(),
                null, null, null
        ));
    }

    /** Manual trigger: PICKED_UP → ENROUTE. Publishes ENROUTE to delivery-events. */
    @Transactional
    public void manualEnroute(String orderId) {
        Delivery delivery = findLatestByOrderId(orderId);
        int etaMinutes = ThreadLocalRandom.current().nextInt(minEtaMinutes, maxEtaMinutes + 1);

        delivery.setStatus(DeliveryStatus.ENROUTE);
        delivery.setEnrouteAt(Instant.now());
        delivery.setEtaMinutes(etaMinutes);
        deliveryRepository.save(delivery);

        log.info("Order {} manually set to ENROUTE (eta {} min)", orderId, etaMinutes);
        eventProducer.publish(new DeliveryEvent(
                UUID.randomUUID().toString(), "ENROUTE", orderId, Instant.now(),
                null, null, etaMinutes
        ));
    }

    /** Manual trigger: ENROUTE → DELIVERED. Publishes DELIVERED to delivery-events. */
    @Transactional
    public void manualDeliver(String orderId) {
        Delivery delivery = findLatestByOrderId(orderId);
        delivery.setStatus(DeliveryStatus.DELIVERED);
        delivery.setDeliveredAt(Instant.now());
        deliveryRepository.save(delivery);

        log.info("Order {} manually set to DELIVERED", orderId);
        eventProducer.publish(new DeliveryEvent(
                UUID.randomUUID().toString(), "DELIVERED", orderId, Instant.now(),
                null, null, null
        ));
    }

    private Delivery findLatestByOrderId(String orderId) {
        return deliveryRepository.findByOrderIdOrderByIdDesc(orderId)
                .stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No delivery record for order " + orderId));
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
