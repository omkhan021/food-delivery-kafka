package com.fooddelivery.notification.service;

import com.fooddelivery.notification.dto.NotificationDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Fans out every notification to ALL currently-connected SSE clients on the global
 * {@code GET /api/notifications/stream} feed (the "Kafka Activity" console in the
 * frontend). This is deliberately a broadcast, not a per-order stream like
 * order-service's {@code /api/orders/{id}/stream} — every connected browser tab sees
 * every event from every topic, which is what makes it useful as a live demo of the
 * whole system's Kafka traffic.
 *
 * <p>Why {@link CopyOnWriteArrayList}: writes (new client connects/disconnects) are rare,
 * reads (broadcasting on every Kafka message) are frequent and happen from the Kafka
 * consumer thread(s) concurrently with HTTP request threads registering/removing
 * emitters. CopyOnWriteArrayList gives lock-free, thread-safe iteration for the hot
 * broadcast path at the cost of an array copy on the rare add/remove — exactly the
 * right trade-off here.
 */
@Component
@Slf4j
public class SseBroadcaster {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    /** Registers a newly-connected client and wires up cleanup on disconnect. */
    public SseEmitter subscribe() {
        // No timeout (0L) — the frontend "Kafka Activity" panel is meant to stay open
        // and streaming indefinitely while the tab is open; browsers/proxies will still
        // close genuinely dead connections, which triggers onError/onCompletion below.
        SseEmitter emitter = new SseEmitter(0L);

        emitter.onCompletion(() -> removeEmitter(emitter));
        emitter.onTimeout(() -> removeEmitter(emitter));
        emitter.onError(ex -> removeEmitter(emitter));

        emitters.add(emitter);
        log.info("SSE client connected. Active clients: {}", emitters.size());
        return emitter;
    }

    private void removeEmitter(SseEmitter emitter) {
        emitters.remove(emitter);
        log.info("SSE client disconnected. Active clients: {}", emitters.size());
    }

    /**
     * Pushes one event (named "notification", per the spec) to every connected client.
     * Any emitter that fails to receive it (client gone, socket closed, etc.) is removed
     * immediately rather than left to error out on the next broadcast.
     */
    public void broadcast(NotificationDto payload) {
        if (emitters.isEmpty()) {
            return;
        }
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("notification")
                        .data(payload));
            } catch (IOException | IllegalStateException ex) {
                log.debug("Failed to send SSE event to a client, removing it: {}", ex.getMessage());
                removeEmitter(emitter);
            }
        }
    }
}
