package com.fooddelivery.kitchen.web;

import com.fooddelivery.kitchen.domain.KitchenOrder;

import java.time.Instant;

/**
 * REST response shape for {@code GET /api/kitchen} and {@code GET /api/kitchen/order/{orderId}}.
 * Kept as a small, explicit projection of {@link KitchenOrder} rather than serializing the
 * JPA entity directly, so the persistence model is free to evolve independently of the API.
 */
public record KitchenOrderResponse(
        Long id,
        String orderId,
        String status,
        Integer estimatedMinutes,
        Instant receivedAt,
        Instant preparingAt,
        Instant preparedAt,
        Instant createdAt,
        Instant updatedAt
) {
    public static KitchenOrderResponse from(KitchenOrder order) {
        return new KitchenOrderResponse(
                order.getId(),
                order.getOrderId(),
                order.getStatus() != null ? order.getStatus().name() : null,
                order.getEstimatedMinutes(),
                order.getReceivedAt(),
                order.getPreparingAt(),
                order.getPreparedAt(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
