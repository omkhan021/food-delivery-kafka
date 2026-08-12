package com.fooddelivery.payment.service;

import com.fooddelivery.payment.domain.Payment;
import com.fooddelivery.payment.event.OrderEvent;
import com.fooddelivery.payment.event.PaymentEvent;
import com.fooddelivery.payment.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DUMMY PAYMENT GATEWAY.
 *
 * This class does not call any real payment processor. It exists purely to demonstrate an
 * "async processing with a delayed outcome" pattern that's common with real payment gateways
 * (and lots of other external systems): you kick off the operation, immediately tell the caller
 * "processing", and some time later - out of band - you find out whether it succeeded or failed
 * and publish that result.
 *
 * Flow for every ORDER_PLACED event:
 *   1. Persist a `payments` row with status PENDING.
 *   2. Publish PAYMENT_PROCESSING to payment-events right away.
 *   3. Schedule a one-shot task on the shared ScheduledExecutorService to run after
 *      payment.processing-delay-ms. That task randomly decides success/failure based on
 *      payment.failure-rate, updates the row to COMPLETED/FAILED, and publishes
 *      PAYMENT_COMPLETED/PAYMENT_FAILED.
 *
 * Step 3 deliberately does NOT run on the Kafka consumer thread - see SchedulerConfig for why.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private static final String TOPIC = "payment-events";

    // A handful of realistic-sounding dummy failure reasons. Rotated round-robin (rather than
    // picked purely at random) purely so repeated demo runs show some variety without repeating
    // the same string many times in a row.
    private static final List<String> FAILURE_REASONS = List.of(
            "card declined (simulated)",
            "insufficient funds (simulated)",
            "payment gateway timeout (simulated)"
    );

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ScheduledExecutorService scheduledExecutorService;
    private final long processingDelayMs;
    private final double failureRate;

    private final AtomicInteger failureReasonCursor = new AtomicInteger(0);

    public PaymentService(PaymentRepository paymentRepository,
                           KafkaTemplate<String, Object> kafkaTemplate,
                           ScheduledExecutorService scheduledExecutorService,
                           @Value("${payment.processing-delay-ms:3000}") long processingDelayMs,
                           @Value("${payment.failure-rate:0.15}") double failureRate) {
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.scheduledExecutorService = scheduledExecutorService;
        this.processingDelayMs = processingDelayMs;
        this.failureRate = failureRate;
    }

    /**
     * Called synchronously from the Kafka listener thread for every ORDER_PLACED event. Does the
     * cheap, fast work (one INSERT, one Kafka send) inline, then hands off the slow/delayed part.
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
        payment = paymentRepository.save(payment);

        log.info("Payment {} created as PENDING for order {}", payment.getId(), orderEvent.getOrderId());

        publishProcessing(orderEvent);

        Long paymentId = payment.getId();
        // Hand the delayed part off to the scheduler thread pool and return immediately - the
        // Kafka listener thread is now free to go poll the next record. See SchedulerConfig for
        // the full rationale on why we never block here.
        scheduledExecutorService.schedule(
                () -> resolvePaymentOutcome(paymentId, orderEvent),
                processingDelayMs,
                TimeUnit.MILLISECONDS);
    }

    private void publishProcessing(OrderEvent orderEvent) {
        PaymentEvent event = new PaymentEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventType("PAYMENT_PROCESSING");
        event.setOrderId(orderEvent.getOrderId());
        event.setTimestamp(Instant.now());
        event.setUserId(orderEvent.getUserId());
        event.setAmount(orderEvent.getPaymentAmount());
        send(orderEvent.getOrderId(), event);
    }

    /**
     * Runs on a payment-gateway-sim-* scheduler thread, NOT the Kafka listener thread. This is
     * the "gateway callback" half of the simulated async payment flow.
     */
    private void resolvePaymentOutcome(Long paymentId, OrderEvent orderEvent) {
        try {
            Payment payment = paymentRepository.findById(paymentId).orElse(null);
            if (payment == null) {
                log.warn("Payment {} for order {} disappeared before outcome could be resolved - skipping",
                        paymentId, orderEvent.getOrderId());
                return;
            }

            boolean failed = ThreadLocalRandom.current().nextDouble() < failureRate;
            Instant now = Instant.now();

            if (failed) {
                String reason = nextFailureReason();
                payment.setStatus(Payment.STATUS_FAILED);
                payment.setFailureReason(reason);
                payment.setUpdatedAt(now);
                paymentRepository.save(payment);

                log.info("Payment {} for order {} FAILED (simulated): {}", paymentId, orderEvent.getOrderId(), reason);
                publishFailed(orderEvent, reason);
            } else {
                String transactionId = "TXN-" + UUID.randomUUID();
                payment.setStatus(Payment.STATUS_COMPLETED);
                payment.setTransactionId(transactionId);
                payment.setUpdatedAt(now);
                paymentRepository.save(payment);

                log.info("Payment {} for order {} COMPLETED (simulated): {}", paymentId, orderEvent.getOrderId(), transactionId);
                publishCompleted(orderEvent, transactionId);
            }
        } catch (Exception ex) {
            // A scheduled one-shot task's exception is otherwise silently swallowed (nothing
            // calls Future.get() on it) - log defensively so a bug here is never invisible.
            log.error("Failed to resolve payment outcome for paymentId={}, orderId={}: {}",
                    paymentId, orderEvent.getOrderId(), ex.getMessage(), ex);
        }
    }

    private void publishCompleted(OrderEvent orderEvent, String transactionId) {
        PaymentEvent event = new PaymentEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventType("PAYMENT_COMPLETED");
        event.setOrderId(orderEvent.getOrderId());
        event.setTimestamp(Instant.now());
        event.setUserId(orderEvent.getUserId());
        event.setAmount(orderEvent.getPaymentAmount());
        event.setTransactionId(transactionId);
        send(orderEvent.getOrderId(), event);
    }

    private void publishFailed(OrderEvent orderEvent, String failureReason) {
        PaymentEvent event = new PaymentEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventType("PAYMENT_FAILED");
        event.setOrderId(orderEvent.getOrderId());
        event.setTimestamp(Instant.now());
        event.setUserId(orderEvent.getUserId());
        event.setAmount(orderEvent.getPaymentAmount());
        event.setFailureReason(failureReason);
        send(orderEvent.getOrderId(), event);
    }

    private void send(String orderId, PaymentEvent event) {
        // Key = orderId, matching ARCHITECTURE.md section 3, so all events for a given order stay
        // in the same partition and are consumed in order.
        kafkaTemplate.send(TOPIC, orderId, event);
    }

    private String nextFailureReason() {
        int index = failureReasonCursor.getAndUpdate(i -> (i + 1) % FAILURE_REASONS.size());
        return FAILURE_REASONS.get(index);
    }
}
