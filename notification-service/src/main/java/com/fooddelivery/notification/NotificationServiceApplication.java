package com.fooddelivery.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * notification-service — pure fan-in Kafka consumer for the Food Delivery Kafka demo.
 *
 * Consumes all 4 domain event topics (order-events, payment-events, kitchen-events,
 * delivery-events) under one consumer group ("notification-service-group"), persists a
 * notification row per event, and broadcasts each one over a global SSE feed for the
 * frontend's "Kafka Activity" console. It never produces to Kafka itself.
 */
@SpringBootApplication
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
