package com.fooddelivery.order.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.order.dto.events.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * order-service's single consumer of {@code payment-events}, {@code kitchen-events} and
 * {@code delivery-events}, running as consumer group {@code order-status-group}
 * (ARCHITECTURE.md sections 3 and 5).
 *
 * <h2>Why one listener for three different event shapes</h2>
 * All three topics carry structurally different JSON payloads (see {@code PaymentEvent},
 * {@code KitchenEvent}, {@code DeliveryEvent} in {@code dto.events}). Rather than run three
 * separate typed listeners, we deserialize the raw record value as a String
 * (see application.yml consumer config) and parse it into a generic {@link JsonNode} with
 * Jackson, then dispatch on {@code record.topic()} + the {@code eventType} field. This keeps one
 * listener id ({@code order-status-group}) subscribed to all three topics, as required by the
 * spec (the replay API looks up listener containers by this id).
 *
 * <h2>Idempotent consumption &amp; replay mechanics</h2>
 * Every record is first upserted into {@code event_log} keyed by
 * (topic, partition, kafka_offset) -- see {@link EventLogService}. The upsert returns
 * {@code times_seen}:
 * <ul>
 *   <li>{@code times_seen == 1}: first time this exact offset has ever been processed. We apply
 *       the real side effects (status transition, history row, SSE push, and any downstream
 *       publish).</li>
 *   <li>{@code times_seen > 1}: this exact offset was already processed before -- either a
 *       consumer-group rebalance redelivering an uncommitted record, or (far more visibly) an
 *       operator-triggered replay via {@code POST /api/admin/kafka/replay} that reset this
 *       group's committed offsets back to "earliest". We still record the replay in
 *       {@code event_log} (that's the whole point -- {@code times_seen} incrementing is the
 *       visible proof that replay worked) but we deliberately skip re-inserting a duplicate
 *       {@code order_status_history} row or pushing a duplicate SSE update. This is the standard
 *       "idempotent consumer" pattern that makes Kafka's at-least-once delivery safe to build a
 *       read-model on top of.</li>
 * </ul>
 *
 * <h2>Saga role: finalizer</h2>
 * This listener is also where the saga terminates: a {@code PAYMENT_FAILED} event short-circuits
 * the order straight to {@code CANCELLED} and republishes {@code ORDER_CANCELLED}; a
 * {@code DELIVERED} event schedules the final {@code COMPLETED} transition (via the shared
 * {@link ScheduledExecutorService}, so we never block this listener's poll thread) and
 * republishes {@code ORDER_COMPLETED}. Both close the loop back onto {@code order-events}, which
 * every other service (notification-service in particular) is also watching.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaEventConsumerService {

    private final ObjectMapper objectMapper;
    private final EventLogService eventLogService;
    private final OrderService orderService;
    private final OrderEventProducer orderEventProducer;
    private final ScheduledExecutorService scheduledExecutorService;

    @Value("${order-service.delivered-to-completed-delay-ms:2000}")
    private long deliveredToCompletedDelayMs;

    @KafkaListener(
            id = "order-status-group",
            groupId = "order-status-group",
            topics = {"payment-events", "kitchen-events", "delivery-events"},
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        // Defensive: one malformed/unexpected message must never crash the listener container and
        // stall the whole consumer group. Log and skip (commit past it) rather than rethrow.
        try {
            handle(record);
        } catch (Exception ex) {
            log.error("[DLQ-LOG] Failed to process record topic={} partition={} offset={} key={}: {}. "
                            + "Skipping (not retried) so the consumer group keeps progressing. "
                            + "In a production system this would be routed to a dead-letter topic.",
                    record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage(), ex);
        }
    }

    private void handle(ConsumerRecord<String, String> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key();

        JsonNode node = objectMapper.readTree(record.value());
        String eventType = node.path("eventType").asText(null);
        String orderId = node.path("orderId").asText(null);

        int timesSeen = eventLogService.upsertAndGetTimesSeen(
                topic, partition, offset, key, eventType, orderId, record.value());

        if (timesSeen > 1) {
            log.info("Replay/redelivery of {}#{}@{} (eventType={}, orderId={}) -- times_seen={} now. "
                            + "Skipping duplicate status-history insert and SSE push (idempotent consumer).",
                    topic, partition, offset, eventType, orderId, timesSeen);
            return;
        }

        log.info("Consuming new event {}#{}@{} eventType={} orderId={}", topic, partition, offset, eventType, orderId);

        if (orderId == null || eventType == null) {
            log.warn("Event on {} missing orderId/eventType, cannot apply a status transition: {}", topic, node);
            return;
        }

        switch (topic) {
            case "payment-events" -> handlePaymentEvent(eventType, orderId, node);
            case "kitchen-events" -> handleKitchenEvent(eventType, orderId, node);
            case "delivery-events" -> handleDeliveryEvent(eventType, orderId, node);
            default -> log.warn("Unexpected topic {} on order-status-group listener", topic);
        }
    }

    private void handlePaymentEvent(String eventType, String orderId, JsonNode node) {
        switch (eventType) {
            case "PAYMENT_PROCESSING" ->
                    orderService.applyStatusTransition(orderId, "PAYMENT_PROCESSING", "payment-events", "Payment processing started");
            case "PAYMENT_COMPLETED" -> {
                String transactionId = node.path("transactionId").asText(null);
                // No dedicated order-status enum value for "payment completed" -- the next visible
                // status change is RECEIVED_BY_KITCHEN once kitchen-service reacts. We still record
                // a history row/note for traceability without changing orders.status.
                orderService.applyStatusTransition(orderId, null, "payment-events",
                        transactionId != null
                                ? "Payment completed (transactionId=" + transactionId + ")"
                                : "Payment completed");
            }
            case "PAYMENT_FAILED" -> {
                String reason = node.path("failureReason").asText("payment failed");
                orderService.applyStatusTransition(orderId, "CANCELLED", "payment-events", "Payment failed: " + reason);
                orderEventProducer.publish(OrderEvent.builder()
                        .eventId(UUID.randomUUID().toString())
                        .eventType("ORDER_CANCELLED")
                        .orderId(orderId)
                        .timestamp(Instant.now())
                        .reason(reason)
                        .build());
            }
            default -> log.warn("Unhandled payment-events eventType={} for order {}", eventType, orderId);
        }
    }

    private void handleKitchenEvent(String eventType, String orderId, JsonNode node) {
        switch (eventType) {
            case "ORDER_RECEIVED" ->
                    orderService.applyStatusTransition(orderId, "RECEIVED_BY_KITCHEN", "kitchen-events", "Order received by kitchen");
            case "PREPARING" -> {
                int estimatedMinutes = node.path("estimatedMinutes").asInt(0);
                orderService.applyStatusTransition(orderId, "PREPARING", "kitchen-events",
                        "Preparing (estimated " + estimatedMinutes + " min)");
            }
            case "PREPARED" ->
                    orderService.applyStatusTransition(orderId, "PREPARED", "kitchen-events", "Order prepared, awaiting pickup");
            default -> log.warn("Unhandled kitchen-events eventType={} for order {}", eventType, orderId);
        }
    }

    private void handleDeliveryEvent(String eventType, String orderId, JsonNode node) {
        switch (eventType) {
            case "DRIVER_ASSIGNED" -> {
                String driverId = node.path("driverId").asText(null);
                String driverName = node.path("driverName").asText(null);
                orderService.applyStatusTransition(orderId, "DRIVER_ASSIGNED", "delivery-events",
                        "Driver " + driverName + " (" + driverId + ") assigned");
            }
            case "PICKED_UP" ->
                    orderService.applyStatusTransition(orderId, "PICKED_UP", "delivery-events", "Order picked up by driver");
            case "ENROUTE" -> {
                int etaMinutes = node.path("etaMinutes").asInt(0);
                orderService.applyStatusTransition(orderId, "ENROUTE", "delivery-events",
                        "Enroute (ETA " + etaMinutes + " min)");
            }
            case "DELIVERED" -> {
                orderService.applyStatusTransition(orderId, "DELIVERED", "delivery-events", "Order delivered");
                scheduleCompletion(orderId);
            }
            default -> log.warn("Unhandled delivery-events eventType={} for order {}", eventType, orderId);
        }
    }

    /**
     * Schedules the final DELIVERED -&gt; COMPLETED transition {@code deliveredToCompletedDelayMs}
     * (default 2000ms) after delivery, on the shared {@link ScheduledExecutorService} so the Kafka
     * listener thread itself is never blocked. This is the point where order-service, as saga
     * finalizer, closes the loop by republishing {@code ORDER_COMPLETED} to {@code order-events}.
     */
    private void scheduleCompletion(String orderId) {
        scheduledExecutorService.schedule(() -> {
            try {
                orderService.applyStatusTransition(orderId, "COMPLETED", null, "Order completed");
                orderEventProducer.publish(OrderEvent.builder()
                        .eventId(UUID.randomUUID().toString())
                        .eventType("ORDER_COMPLETED")
                        .orderId(orderId)
                        .timestamp(Instant.now())
                        .build());
            } catch (Exception ex) {
                log.error("Failed to complete order {} after DELIVERED->COMPLETED delay: {}", orderId, ex.getMessage(), ex);
            }
        }, deliveredToCompletedDelayMs, TimeUnit.MILLISECONDS);
    }
}
