package com.fooddelivery.kitchen.repository;

import com.fooddelivery.kitchen.domain.KitchenOrder;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface KitchenOrderRepository extends JpaRepository<KitchenOrder, Long> {

    Optional<KitchenOrder> findByOrderId(String orderId);

    List<KitchenOrder> findAllByOrderByIdDesc();
}
