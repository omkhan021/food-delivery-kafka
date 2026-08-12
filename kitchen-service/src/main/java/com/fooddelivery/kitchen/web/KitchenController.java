package com.fooddelivery.kitchen.web;

import com.fooddelivery.kitchen.domain.KitchenOrder;
import com.fooddelivery.kitchen.repository.KitchenOrderRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * Read-only REST API for kitchen-service (ARCHITECTURE.md section 8). This service's
 * data is entirely written via the Kafka-driven state machine in
 * {@code KitchenOrderService} - there is no write endpoint here by design.
 */
@RestController
@RequestMapping("/api/kitchen")
public class KitchenController {

    private final KitchenOrderRepository repository;

    public KitchenController(KitchenOrderRepository repository) {
        this.repository = repository;
    }

    /** GET /api/kitchen - all kitchen orders, newest first. */
    @GetMapping
    public List<KitchenOrderResponse> listAll() {
        return repository.findAllByOrderByIdDesc().stream()
                .map(KitchenOrderResponse::from)
                .toList();
    }

    /** GET /api/kitchen/order/{orderId} - the kitchen record for a single order. */
    @GetMapping("/order/{orderId}")
    public ResponseEntity<KitchenOrderResponse> getByOrderId(@PathVariable String orderId) {
        KitchenOrder order = repository.findByOrderId(orderId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "No kitchen order found for orderId=" + orderId));
        return ResponseEntity.ok(KitchenOrderResponse.from(order));
    }
}
