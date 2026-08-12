package com.fooddelivery.delivery.controller;

import com.fooddelivery.delivery.entity.Delivery;
import com.fooddelivery.delivery.service.DeliveryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only REST API for deliveries, per ARCHITECTURE.md section 8. There is no write endpoint —
 * every row is created and progressed exclusively via the Kafka-driven state machine in
 * {@link DeliveryService}.
 */
@RestController
@RequestMapping("/api/deliveries")
public class DeliveryController {

    private final DeliveryService deliveryService;

    public DeliveryController(DeliveryService deliveryService) {
        this.deliveryService = deliveryService;
    }

    /** Lists all deliveries, newest first. */
    @GetMapping
    public List<Delivery> listAll() {
        return deliveryService.findAllNewestFirst();
    }

    /**
     * Deliveries for a given order, newest first. Returned as a list (rather than a single
     * object) because a topic replay could in principle redeliver a PREPARED event and create
     * more than one row for the same order - the list makes that visible instead of silently
     * picking one.
     */
    @GetMapping("/order/{orderId}")
    public List<Delivery> findByOrderId(@PathVariable String orderId) {
        return deliveryService.findByOrderId(orderId);
    }
}
