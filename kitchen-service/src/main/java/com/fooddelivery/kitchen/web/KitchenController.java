package com.fooddelivery.kitchen.web;

import com.fooddelivery.kitchen.domain.KitchenOrder;
import com.fooddelivery.kitchen.repository.KitchenOrderRepository;
import com.fooddelivery.kitchen.service.KitchenOrderService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/api/kitchen")
public class KitchenController {

    private final KitchenOrderRepository repository;
    private final KitchenOrderService kitchenOrderService;

    public KitchenController(KitchenOrderRepository repository,
                             KitchenOrderService kitchenOrderService) {
        this.repository = repository;
        this.kitchenOrderService = kitchenOrderService;
    }

    /** GET /api/kitchen — all kitchen orders, newest first. */
    @GetMapping
    public List<KitchenOrderResponse> listAll() {
        return repository.findAllByOrderByIdDesc().stream()
                .map(KitchenOrderResponse::from)
                .toList();
    }

    /** GET /api/kitchen/order/{orderId} — kitchen record for a single order. */
    @GetMapping("/order/{orderId}")
    public ResponseEntity<KitchenOrderResponse> getByOrderId(@PathVariable String orderId) {
        KitchenOrder order = repository.findByOrderId(orderId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "No kitchen order found for orderId=" + orderId));
        return ResponseEntity.ok(KitchenOrderResponse.from(order));
    }

    /**
     * POST /api/kitchen/{orderId}/prepare
     * Manual trigger: transitions RECEIVED → PREPARING, publishes PREPARING to kitchen-events.
     */
    @PostMapping("/{orderId}/prepare")
    public ResponseEntity<Void> prepare(@PathVariable String orderId) {
        kitchenOrderService.manualPrepare(orderId);
        return ResponseEntity.ok().build();
    }

    /**
     * POST /api/kitchen/{orderId}/ready
     * Manual trigger: transitions PREPARING → PREPARED, publishes PREPARED to kitchen-events.
     */
    @PostMapping("/{orderId}/ready")
    public ResponseEntity<Void> ready(@PathVariable String orderId) {
        kitchenOrderService.manualReady(orderId);
        return ResponseEntity.ok().build();
    }
}
