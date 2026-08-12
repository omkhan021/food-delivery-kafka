package com.fooddelivery.order.repository;

import com.fooddelivery.order.domain.EventLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface EventLogRepository extends JpaRepository<EventLog, Long> {
    List<EventLog> findByOrderIdOrderByFirstSeenAtAsc(String orderId);
}
