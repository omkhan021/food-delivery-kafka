package com.fooddelivery.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Payment Service — Food Delivery Kafka Demo.
 *
 * This is a DUMMY payment gateway: it does not talk to a real payment processor. It consumes
 * ORDER_PLACED events from Kafka, simulates a processing delay, and then randomly decides
 * success/failure to emit PAYMENT_COMPLETED or PAYMENT_FAILED. See {@link service.PaymentService}
 * for the simulation logic and {@link listener.OrderEventListener} for the Kafka consumer.
 */
@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
