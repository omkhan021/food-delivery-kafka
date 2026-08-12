package com.fooddelivery.notification.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

/**
 * Turns a raw event (topic + JsonNode payload) into a short, human-readable sentence for
 * the notification log / activity feed. This is the one place in the service that needs
 * to know the field names used by each of the 4 topic-specific event shapes described in
 * ARCHITECTURE.md sections 4.1-4.4 — everything else in the service only deals with the
 * 3 common fields (eventType, orderId, timestamp) plus this derived message string.
 */
@Component
public class NotificationMessageFactory {

    public String buildMessage(String topic, String eventType, JsonNode node) {
        String orderId = text(node, "orderId");
        return switch (topic) {
            case "order-events" -> orderEventMessage(eventType, orderId, node);
            case "payment-events" -> paymentEventMessage(eventType, orderId, node);
            case "kitchen-events" -> kitchenEventMessage(eventType, orderId, node);
            case "delivery-events" -> deliveryEventMessage(eventType, orderId, node);
            default -> "Event " + eventType + " for order " + orderId + " on topic " + topic;
        };
    }

    // ---- order-events (4.1) ----------------------------------------------------------
    private String orderEventMessage(String eventType, String orderId, JsonNode node) {
        if (eventType == null) {
            return "Unknown order event for order " + orderId;
        }
        return switch (eventType) {
            case "ORDER_PLACED" -> {
                String userId = text(node, "userId");
                String amount = money(node, "paymentAmount");
                yield "Order %s placed by %s for $%s".formatted(orderId, userId, amount);
            }
            case "ORDER_COMPLETED" -> "Order %s completed".formatted(orderId);
            case "ORDER_CANCELLED" -> {
                String reason = text(node, "reason");
                yield reason != null
                        ? "Order %s cancelled (%s)".formatted(orderId, reason)
                        : "Order %s cancelled".formatted(orderId);
            }
            default -> "Order %s: %s".formatted(orderId, eventType);
        };
    }

    // ---- payment-events (4.2) ---------------------------------------------------------
    private String paymentEventMessage(String eventType, String orderId, JsonNode node) {
        if (eventType == null) {
            return "Unknown payment event for order " + orderId;
        }
        return switch (eventType) {
            case "PAYMENT_PROCESSING" -> "Payment processing for order %s".formatted(orderId);
            case "PAYMENT_COMPLETED" -> {
                String txn = text(node, "transactionId");
                yield "Payment completed for order %s (txn %s)".formatted(orderId, txn);
            }
            case "PAYMENT_FAILED" -> {
                String reason = text(node, "failureReason");
                yield reason != null
                        ? "Payment failed for order %s (%s)".formatted(orderId, reason)
                        : "Payment failed for order %s".formatted(orderId);
            }
            default -> "Payment for order %s: %s".formatted(orderId, eventType);
        };
    }

    // ---- kitchen-events (4.3) ----------------------------------------------------------
    private String kitchenEventMessage(String eventType, String orderId, JsonNode node) {
        if (eventType == null) {
            return "Unknown kitchen event for order " + orderId;
        }
        return switch (eventType) {
            case "ORDER_RECEIVED" -> "Order %s received by the kitchen".formatted(orderId);
            case "PREPARING" -> {
                JsonNode minutesNode = node.get("estimatedMinutes");
                yield minutesNode != null && !minutesNode.isNull()
                        ? "Order %s is now being prepared (~%s min)".formatted(orderId, minutesNode.asText())
                        : "Order %s is now being prepared".formatted(orderId);
            }
            case "PREPARED" -> "Order %s is ready for pickup".formatted(orderId);
            default -> "Kitchen update for order %s: %s".formatted(orderId, eventType);
        };
    }

    // ---- delivery-events (4.4) ----------------------------------------------------------
    private String deliveryEventMessage(String eventType, String orderId, JsonNode node) {
        if (eventType == null) {
            return "Unknown delivery event for order " + orderId;
        }
        return switch (eventType) {
            case "DRIVER_ASSIGNED" -> {
                String driverName = text(node, "driverName");
                yield "Driver %s assigned to order %s".formatted(driverName, orderId);
            }
            case "PICKED_UP" -> "Order %s picked up by the driver".formatted(orderId);
            case "ENROUTE" -> {
                JsonNode etaNode = node.get("etaMinutes");
                yield etaNode != null && !etaNode.isNull()
                        ? "Order %s is on the way (ETA %s min)".formatted(orderId, etaNode.asText())
                        : "Order %s is on the way".formatted(orderId);
            }
            case "DELIVERED" -> "Order %s delivered!".formatted(orderId);
            default -> "Delivery update for order %s: %s".formatted(orderId, eventType);
        };
    }

    private String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    /** Formats a numeric field to 2 decimal places for money-ish display (e.g. "42.50"). */
    private String money(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return "0.00";
        }
        if (value.isNumber()) {
            return String.format("%.2f", value.asDouble());
        }
        return value.asText();
    }
}
