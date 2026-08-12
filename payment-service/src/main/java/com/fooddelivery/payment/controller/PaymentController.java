package com.fooddelivery.payment.controller;

import com.fooddelivery.payment.domain.Payment;
import com.fooddelivery.payment.repository.PaymentRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentRepository paymentRepository;

    public PaymentController(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    /** GET /api/payments - all payments, newest first. */
    @GetMapping
    public List<Payment> listAll() {
        return paymentRepository.findAllByOrderByCreatedAtDesc();
    }

    /** GET /api/payments/order/{orderId} - the payment record for a single order, if any. */
    @GetMapping("/order/{orderId}")
    public ResponseEntity<Payment> byOrderId(@PathVariable String orderId) {
        return paymentRepository.findFirstByOrderIdOrderByCreatedAtDesc(orderId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
