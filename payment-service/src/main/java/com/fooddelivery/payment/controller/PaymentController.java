package com.fooddelivery.payment.controller;

import com.fooddelivery.payment.domain.Payment;
import com.fooddelivery.payment.repository.PaymentRepository;
import com.fooddelivery.payment.service.PaymentService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentRepository paymentRepository;
    private final PaymentService paymentService;

    public PaymentController(PaymentRepository paymentRepository,
                             PaymentService paymentService) {
        this.paymentRepository = paymentRepository;
        this.paymentService = paymentService;
    }

    /** GET /api/payments — all payments, newest first. */
    @GetMapping
    public List<Payment> listAll() {
        return paymentRepository.findAllByOrderByCreatedAtDesc();
    }

    /** GET /api/payments/order/{orderId} — payment record for a single order. */
    @GetMapping("/order/{orderId}")
    public ResponseEntity<Payment> byOrderId(@PathVariable String orderId) {
        return paymentRepository.findFirstByOrderIdOrderByCreatedAtDesc(orderId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * POST /api/payments/{orderId}/complete
     * Manual trigger: publishes PAYMENT_COMPLETED to payment-events.
     * Called by the frontend "Pay Success" button on the Track Order panel.
     */
    @PostMapping("/{orderId}/complete")
    public ResponseEntity<Void> manualComplete(@PathVariable String orderId) {
        paymentService.manualComplete(orderId);
        return ResponseEntity.ok().build();
    }

    /**
     * POST /api/payments/{orderId}/fail
     * Manual trigger: publishes PAYMENT_FAILED to payment-events.
     * Called by the frontend "Pay Fail" button on the Track Order panel.
     */
    @PostMapping("/{orderId}/fail")
    public ResponseEntity<Void> manualFail(@PathVariable String orderId) {
        paymentService.manualFail(orderId);
        return ResponseEntity.ok().build();
    }
}
