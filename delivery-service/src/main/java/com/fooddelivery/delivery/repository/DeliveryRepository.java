package com.fooddelivery.delivery.repository;

import com.fooddelivery.delivery.entity.Delivery;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface DeliveryRepository extends JpaRepository<Delivery, Long> {

    List<Delivery> findAllByOrderByIdDesc();

    /**
     * A given order only ever gets one delivery row in this simple demo (one PREPARED event ->
     * one driver assignment), but we look up the most recent one defensively in case a topic
     * replay ever produces a duplicate PREPARED event for the same order.
     */
    Optional<Delivery> findFirstByOrderIdOrderByIdDesc(String orderId);

    List<Delivery> findByOrderIdOrderByIdDesc(String orderId);
}
