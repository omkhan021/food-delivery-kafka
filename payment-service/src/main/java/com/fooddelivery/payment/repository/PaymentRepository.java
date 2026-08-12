package com.fooddelivery.payment.repository;

import com.fooddelivery.payment.domain.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, Long> {

    List<Payment> findAllByOrderByCreatedAtDesc();

    // In this demo there's exactly one payment attempt per order, but querying "most recent
    // first" keeps this safe even if that assumption ever changes.
    Optional<Payment> findFirstByOrderIdOrderByCreatedAtDesc(String orderId);
}
