package com.fooddelivery.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * order-service — saga initiator AND finalizer for the Food Delivery Kafka Demo.
 *
 * <p>Role in the saga (see ARCHITECTURE.md section 5):
 * <ul>
 *   <li>Initiator: accepts {@code POST /api/orders}, persists the order, publishes
 *       {@code ORDER_PLACED} to {@code order-events}.</li>
 *   <li>Finalizer: consumes {@code payment-events}, {@code kitchen-events} and
 *       {@code delivery-events} to build a read-model of order status, and closes the loop by
 *       publishing {@code ORDER_CANCELLED} (on payment failure) or {@code ORDER_COMPLETED}
 *       (after delivery) back to {@code order-events}.</li>
 * </ul>
 */
@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
