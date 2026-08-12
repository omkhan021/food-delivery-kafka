package com.fooddelivery.notification.web;

import com.fooddelivery.notification.dto.NotificationDto;
import com.fooddelivery.notification.service.NotificationService;
import com.fooddelivery.notification.service.SseBroadcaster;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;

/**
 * REST + SSE surface for notification-service, per ARCHITECTURE.md section 8.
 */
@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationService notificationService;
    private final SseBroadcaster broadcaster;

    /** Newest-first, capped at {@code notification.max-list-size} (default 200) rows. */
    @GetMapping
    public List<NotificationDto> listRecent() {
        return notificationService.listRecent();
    }

    @GetMapping("/order/{orderId}")
    public List<NotificationDto> listByOrder(@PathVariable String orderId) {
        return notificationService.listByOrderId(orderId);
    }

    /**
     * Global live activity feed. Every browser tab that opens this connection receives
     * every event from every topic (see {@link SseBroadcaster} for why this is a
     * broadcast list rather than a per-order map like order-service's tracking stream).
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return broadcaster.subscribe();
    }
}
