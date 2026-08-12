package com.fooddelivery.order.repository;

import com.fooddelivery.order.domain.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OrderRepository extends JpaRepository<Order, String> {

    @Query("select o from Order o order by o.createdAt desc")
    List<Order> findAllNewestFirst();
}
