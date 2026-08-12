package com.fooddelivery.order.dto.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/** Raw {@code event_log} row, returned by {@code GET /api/orders/{orderId}/event-log}. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EventLogResponse {
    private Long id;
    private String topic;
    private int partition;
    private long kafkaOffset;
    private String eventKey;
    private String eventType;
    private String orderId;
    private String payload;
    private int timesSeen;
    private Instant firstSeenAt;
    private Instant lastSeenAt;
}
