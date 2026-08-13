package com.fooddelivery.delivery.controller;

import com.fooddelivery.delivery.entity.Delivery;
import com.fooddelivery.delivery.service.DeliveryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/deliveries")
public class DeliveryController {

    private final DeliveryService deliveryService;

    public DeliveryController(DeliveryService deliveryService) {
        this.deliveryService = deliveryService;
    }

    /** GET /api/deliveries — all deliveries, newest first. */
    @GetMapping
    public List<Delivery> listAll() {
        return deliveryService.findAllNewestFirst();
    }

    /** GET /api/deliveries/order/{orderId} — deliveries for a given order. */
    @GetMapping("/order/{orderId}")
    public List<Delivery> findByOrderId(@PathVariable String orderId) {
        return deliveryService.findByOrderId(orderId);
    }

    /**
     * POST /api/deliveries/{orderId}/pickup
     * Manual trigger: ASSIGNED → PICKED_UP, publishes PICKED_UP to delivery-events.
     */
    @PostMapping("/{orderId}/pickup")
    public ResponseEntity<Void> pickup(@PathVariable String orderId) {
        deliveryService.manualPickup(orderId);
        return ResponseEntity.ok().build();
    }

    /**
     * POST /api/deliveries/{orderId}/enroute
     * Manual trigger: PICKED_UP → ENROUTE, publishes ENROUTE to delivery-events.
     */
    @PostMapping("/{orderId}/enroute")
    public ResponseEntity<Void> enroute(@PathVariable String orderId) {
        deliveryService.manualEnroute(orderId);
        return ResponseEntity.ok().build();
    }

    /**
     * POST /api/deliveries/{orderId}/deliver
     * Manual trigger: ENROUTE → DELIVERED, publishes DELIVERED to delivery-events.
     */
    @PostMapping("/{orderId}/deliver")
    public ResponseEntity<Void> deliver(@PathVariable String orderId) {
        deliveryService.manualDeliver(orderId);
        return ResponseEntity.ok().build();
    }
}
