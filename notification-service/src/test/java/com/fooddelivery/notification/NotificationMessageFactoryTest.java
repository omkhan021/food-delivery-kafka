package com.fooddelivery.notification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.notification.service.NotificationMessageFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NotificationMessageFactoryTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final NotificationMessageFactory factory = new NotificationMessageFactory();

    @Test
    void orderPlacedMessage() throws Exception {
        JsonNode node = mapper.readTree("""
                {"eventType":"ORDER_PLACED","orderId":"ORD-123","userId":"user-101","paymentAmount":42.5}
                """);
        String msg = factory.buildMessage("order-events", "ORDER_PLACED", node);
        assertEquals("Order ORD-123 placed by user-101 for $42.50", msg);
    }

    @Test
    void paymentCompletedMessage() throws Exception {
        JsonNode node = mapper.readTree("""
                {"eventType":"PAYMENT_COMPLETED","orderId":"ORD-123","transactionId":"TXN-abcd"}
                """);
        String msg = factory.buildMessage("payment-events", "PAYMENT_COMPLETED", node);
        assertEquals("Payment completed for order ORD-123 (txn TXN-abcd)", msg);
    }

    @Test
    void kitchenPreparingMessage() throws Exception {
        JsonNode node = mapper.readTree("""
                {"eventType":"PREPARING","orderId":"ORD-123","estimatedMinutes":15}
                """);
        String msg = factory.buildMessage("kitchen-events", "PREPARING", node);
        assertEquals("Order ORD-123 is now being prepared (~15 min)", msg);
    }

    @Test
    void deliveryDriverAssignedMessage() throws Exception {
        JsonNode node = mapper.readTree("""
                {"eventType":"DRIVER_ASSIGNED","orderId":"ORD-123","driverId":"DRV-3","driverName":"Sam Rivera"}
                """);
        String msg = factory.buildMessage("delivery-events", "DRIVER_ASSIGNED", node);
        assertEquals("Driver Sam Rivera assigned to order ORD-123", msg);
    }

    @Test
    void deliveredMessage() throws Exception {
        JsonNode node = mapper.readTree("""
                {"eventType":"DELIVERED","orderId":"ORD-123"}
                """);
        String msg = factory.buildMessage("delivery-events", "DELIVERED", node);
        assertEquals("Order ORD-123 delivered!", msg);
    }

    @Test
    void unknownEventTypeFallsBackGracefully() throws Exception {
        JsonNode node = mapper.readTree("""
                {"eventType":"SOMETHING_NEW","orderId":"ORD-999"}
                """);
        String msg = factory.buildMessage("order-events", "SOMETHING_NEW", node);
        assertTrue(msg.contains("ORD-999"));
        assertTrue(msg.contains("SOMETHING_NEW"));
    }
}
