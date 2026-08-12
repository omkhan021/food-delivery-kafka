package com.fooddelivery.notification.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fooddelivery.notification.model.Notification;

import java.time.Instant;

/**
 * Wire shape for both the REST list endpoints and the SSE {@code notification} event
 * payload: {@code {orderId, sourceTopic, eventType, message, timestamp}}, exactly as
 * specified in ARCHITECTURE.md section 8.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record NotificationDto(
        Long id,
        String orderId,
        String sourceTopic,
        String eventType,
        String message,
        Instant timestamp
) {

    public static NotificationDto from(Notification n) {
        return new NotificationDto(
                n.getId(),
                n.getOrderId(),
                n.getSourceTopic(),
                n.getEventType(),
                n.getMessage(),
                n.getCreatedAt() == null ? null : n.getCreatedAt().toInstant()
        );
    }

    /** Broadcast-only variant (no DB id yet) used the instant an event is consumed. */
    public static NotificationDto broadcastOf(String orderId, String sourceTopic, String eventType,
                                                String message, Instant timestamp) {
        return new NotificationDto(null, orderId, sourceTopic, eventType, message, timestamp);
    }
}
