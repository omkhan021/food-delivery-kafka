package com.fooddelivery.order.service;

import com.fooddelivery.order.dto.api.StatusStreamEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Per-order Server-Sent-Events fan-out, backing {@code GET /api/orders/{orderId}/stream}.
 *
 * <p>Uses a {@code ConcurrentHashMap<String, List<SseEmitter>>} as instructed by
 * ARCHITECTURE.md section 8: one entry per orderId, fanning a single status update out to every
 * browser tab currently tracking that order.
 */
@Service
@Slf4j
public class SseEmitterService {

    private static final long EMITTER_TIMEOUT_MS = 30 * 60 * 1000L; // 30 minutes

    private final ConcurrentHashMap<String, List<SseEmitter>> emittersByOrderId = new ConcurrentHashMap<>();

    public SseEmitter subscribe(String orderId) {
        SseEmitter emitter = new SseEmitter(EMITTER_TIMEOUT_MS);
        List<SseEmitter> emitters = emittersByOrderId.computeIfAbsent(orderId, k -> new CopyOnWriteArrayList<>());
        emitters.add(emitter);

        emitter.onCompletion(() -> removeEmitter(orderId, emitter));
        emitter.onTimeout(() -> removeEmitter(orderId, emitter));
        emitter.onError(ex -> removeEmitter(orderId, emitter));

        return emitter;
    }

    private void removeEmitter(String orderId, SseEmitter emitter) {
        List<SseEmitter> emitters = emittersByOrderId.get(orderId);
        if (emitters != null) {
            emitters.remove(emitter);
            if (emitters.isEmpty()) {
                emittersByOrderId.remove(orderId, emitters);
            }
        }
    }

    /** Pushes a {@code status} event to every open SSE connection for the given order. */
    public void push(String orderId, StatusStreamEvent event) {
        List<SseEmitter> emitters = emittersByOrderId.get(orderId);
        if (emitters == null || emitters.isEmpty()) {
            return;
        }
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("status").data(event));
            } catch (IOException | IllegalStateException ex) {
                log.debug("Dropping dead SSE emitter for order {}: {}", orderId, ex.getMessage());
                removeEmitter(orderId, emitter);
            }
        }
    }
}
