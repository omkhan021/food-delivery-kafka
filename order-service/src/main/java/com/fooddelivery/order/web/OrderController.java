package com.fooddelivery.order.web;

import com.fooddelivery.order.dto.api.CreateOrderRequest;
import com.fooddelivery.order.dto.api.EventLogResponse;
import com.fooddelivery.order.dto.api.OrderResponse;
import com.fooddelivery.order.service.OrderService;
import com.fooddelivery.order.service.SseEmitterService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;

/**
 * order-service's core REST API (ARCHITECTURE.md section 8): place an order (saga entry point),
 * read the order/status read-model, and stream live status updates over SSE.
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final SseEmitterService sseEmitterService;

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        OrderResponse response = orderService.createOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping
    public List<OrderResponse> listOrders() {
        return orderService.listOrders();
    }

    @GetMapping("/{orderId}")
    public OrderResponse getOrder(@PathVariable String orderId) {
        return orderService.getOrder(orderId);
    }

    @GetMapping(value = "/{orderId}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamOrderStatus(@PathVariable String orderId) {
        return sseEmitterService.subscribe(orderId);
    }

    @GetMapping("/{orderId}/event-log")
    public List<EventLogResponse> getEventLog(@PathVariable String orderId) {
        return orderService.getEventLog(orderId);
    }
}
