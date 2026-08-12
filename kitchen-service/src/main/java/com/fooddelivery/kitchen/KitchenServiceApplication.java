package com.fooddelivery.kitchen;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * kitchen-service - simulates a restaurant kitchen reacting to completed payments.
 *
 * <p>Kafka role in the wider Food Delivery Kafka Demo:
 * <ul>
 *   <li>Consumes {@code PAYMENT_COMPLETED} events from topic {@code payment-events}
 *       (consumer group {@code kitchen-service-group}).</li>
 *   <li>Runs a short, non-blocking, delayed state machine (RECEIVED -> PREPARING -> PREPARED)
 *       and publishes one {@code KitchenEvent} per transition to topic {@code kitchen-events}.</li>
 * </ul>
 * See ARCHITECTURE.md at the repo root for the full contract.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class KitchenServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(KitchenServiceApplication.class, args);
    }
}
