package com.fooddelivery.notification.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fooddelivery.notification.dto.NotificationDto;
import com.fooddelivery.notification.model.Notification;
import com.fooddelivery.notification.repository.NotificationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Core fan-in logic shared by the Kafka listener and the REST layer: persist one
 * {@link Notification} row per consumed event, then broadcast it to every connected SSE
 * client via {@link SseBroadcaster}. Persistence happens first so the notification log
 * (queryable via REST) and the live feed never disagree about what has been recorded.
 */
@Service
@Slf4j
public class NotificationService {

    private final NotificationRepository repository;
    private final NotificationMessageFactory messageFactory;
    private final SseBroadcaster broadcaster;
    private final int maxListSize;

    public NotificationService(NotificationRepository repository,
                                NotificationMessageFactory messageFactory,
                                SseBroadcaster broadcaster,
                                @Value("${notification.max-list-size:200}") int maxListSize) {
        this.repository = repository;
        this.messageFactory = messageFactory;
        this.broadcaster = broadcaster;
        this.maxListSize = maxListSize;
    }

    /**
     * Handles one raw Kafka record: builds the human-readable message, persists it, and
     * broadcasts it. Called from the Kafka listener for all 4 topics alike.
     */
    @Transactional
    public void handleEvent(String topic, JsonNode payload) {
        String eventType = textOrNull(payload, "eventType");
        String orderId = textOrNull(payload, "orderId");
        Instant eventTimestamp = parseTimestamp(payload);

        if (eventType == null) {
            // Still record it (with a placeholder eventType) rather than silently
            // dropping a message just because it doesn't look like our other events —
            // a malformed/unexpected message must not crash the listener, but it
            // shouldn't vanish either.
            log.warn("Event on topic {} is missing eventType field: {}", topic, payload);
            eventType = "UNKNOWN";
        }

        String message = messageFactory.buildMessage(topic, eventType, payload);

        Notification notification = new Notification();
        notification.setOrderId(orderId);
        notification.setSourceTopic(topic);
        notification.setEventType(eventType);
        notification.setMessage(message);
        notification.setCreatedAt(OffsetDateTime.now(ZoneOffset.UTC));

        Notification saved = repository.save(notification);
        log.info("[{}] {} -> {}", topic, eventType, message);

        // Broadcast using the source event's own timestamp when present (falls back to
        // "now") so the activity feed reflects when the upstream event actually happened.
        NotificationDto dto = NotificationDto.broadcastOf(
                orderId, topic, notification.getEventType(), message,
                eventTimestamp != null ? eventTimestamp : saved.getCreatedAt().toInstant());
        broadcaster.broadcast(dto);
    }

    @Transactional(readOnly = true)
    public List<NotificationDto> listRecent() {
        return repository.findAllByOrderByCreatedAtDesc(PageRequest.of(0, maxListSize))
                .stream()
                .map(NotificationDto::from)
                .toList();
    }

    @Transactional(readOnly = true)
    public List<NotificationDto> listByOrderId(String orderId) {
        return repository.findByOrderIdOrderByCreatedAtDesc(orderId)
                .stream()
                .map(NotificationDto::from)
                .toList();
    }

    private String textOrNull(JsonNode node, String field) {
        if (node == null) {
            return null;
        }
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private Instant parseTimestamp(JsonNode node) {
        String raw = textOrNull(node, "timestamp");
        if (raw == null) {
            return null;
        }
        try {
            return Instant.parse(raw);
        } catch (Exception ex) {
            log.debug("Could not parse event timestamp '{}': {}", raw, ex.getMessage());
            return null;
        }
    }
}
