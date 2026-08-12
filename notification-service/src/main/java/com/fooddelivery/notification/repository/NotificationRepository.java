package com.fooddelivery.notification.repository;

import com.fooddelivery.notification.model.Notification;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface NotificationRepository extends JpaRepository<Notification, Long> {

    List<Notification> findAllByOrderByCreatedAtDesc(Pageable pageable);

    List<Notification> findByOrderIdOrderByCreatedAtDesc(String orderId);
}
