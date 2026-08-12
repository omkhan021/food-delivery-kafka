package com.fooddelivery.order.dto.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Payload pushed over the per-order SSE stream ({@code GET /api/orders/{orderId}/stream}),
 * event name {@code status}. Shape per ARCHITECTURE.md section 8.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StatusStreamEvent {
    private String orderId;
    private String status;
    private String note;
    private Instant timestamp;
}
