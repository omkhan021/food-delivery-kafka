package com.fooddelivery.payment.service;

import com.fooddelivery.payment.domain.Payment;
import com.fooddelivery.payment.event.OrderEvent;
import com.fooddelivery.payment.event.PaymentEvent;
import com.fooddelivery.payment.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manual-mode payment gateway simulator.
 *
 * When an ORDER_PLACED arrives, we persist the payment row and immediately publish
 * PAYMENT_PROCESSING (the "gateway acknowledged" event). We then stop and wait for a
 * manual REST call from the frontend — either /complete or /fail — before publishing
 * the outcome. This lets the user watch each Kafka event appear in Kafka-UI one at a time.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final String TOPIC = "payment-events";

    private static final List<String> FAILURE_REASONS = List.of(
            "card declined (simulated)",
            "insufficient funds (simulated)",
            "payment gateway timeout (simulated)"
    );

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AtomicInteger failureReasonCursor = new AtomicInteger(0);

    public PaymentService(PaymentRepository paymentRepository,
                          KafkaTemplate<String, Object> kafkaTemplate) {
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Called by the Kafka listener for every ORDER_PLACED event.
     * Persists the payment row as PENDING and publishes PAYMENT_PROCESSING, then returns.
     * The outcome (COMPLETED / FAILED) is triggered manually via REST.
     */
    public void handleOrderPlaced(OrderEvent orderEvent) {
        Instant now = Instant.now();

        Payment payment = new Payment();
        payment.setOrderId(orderEvent.getOrderId());
        payment.setUserId(orderEvent.getUserId());
        payment.setAmount(orderEvent.getPaymentAmount());
        payment.setStatus(Payment.STATUS_PENDING);
        payment.setCreatedAt(now);
        payment.setUpdatedAt(now);
        paymentRepository.save(payment);

        log.info("Payment created as PENDING for order {}", orderEvent.getOrderId());

        PaymentEvent processing = new PaymentEvent();
        processing.setEventId(UUID.randomUUID().toString());
        processing.setEventType("PAYMENT_PROCESSING");
        processing.setOrderId(orderEvent.getOrderId());
        processing.setTimestamp(Instant.now());
        processing.setUserId(orderEvent.getUserId());
        processing.setAmount(orderEvent.getPaymentAmount());
        send(orderEvent.getOrderId(), processing);
    }

    /** Manual trigger: mark payment succeeded, publish PAYMENT_COMPLETED. */
    public void manualComplete(String orderId) {
        Payment payment = paymentRepository
                .findFirstByOrderIdOrderByCreatedAtDesc(orderId)
                .orElseThrow(() -> new IllegalStateException("No payment record for order " + orderId));

        String txnId = "TXN-" + UUID.randomUUID();
        payment.setStatus(Payment.STATUS_COMPLETED);
        payment.setTransactionId(txnId);
        payment.setUpdatedAt(Instant.now());
        paymentRepository.save(payment);

        log.info("Payment for order {} manually COMPLETED, txn={}", orderId, txnId);

        PaymentEvent event = new PaymentEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventType("PAYMENT_COMPLETED");
        event.setOrderId(orderId);
        event.setTimestamp(Instant.now());
        event.setUserId(payment.getUserId());
        event.setAmount(payment.getAmount());
        event.setTransactionId(txnId);
        send(orderId, event);
    }

    /** Manual trigger: mark payment failed, publish PAYMENT_FAILED. */
    public void manualFail(String orderId) {
        Payment payment = paymentRepository
                .findFirstByOrderIdOrderByCreatedAtDesc(orderId)
                .orElseThrow(() -> new IllegalStateException("No payment record for order " + orderId));

        String reason = nextFailureReason();
        payment.setStatus(Payment.STATUS_FAILED);
        payment.setFailureReason(reason);
        payment.setUpdatedAt(Instant.now());
        paymentRepository.save(payment);

        log.info("Payment for order {} manually FAILED: {}", orderId, reason);

        PaymentEvent event = new PaymentEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventType("PAYMENT_FAILED");
        event.setOrderId(orderId);
        event.setTimestamp(Instant.now());
        event.setUserId(payment.getUserId());
        event.setAmount(payment.getAmount());
        event.setFailureReason(reason);
        send(orderId, event);
    }

    private void send(String orderId, PaymentEvent event) {
        kafkaTemplate.send(TOPIC, orderId, event);
    }

    private String nextFailureReason() {
        int index = failureReasonCursor.getAndUpdate(i -> (i + 1) % FAILURE_REASONS.size());
        return FAILURE_REASONS.get(index);
    }
}
