package com.fooddelivery.notification.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * JPA entity mapped to {@code notification_service.notifications}, created by Flyway
 * (see {@code db/migration/V1__init.sql}). Hibernate's ddl-auto is "validate" only —
 * this class must stay in lock-step with the migration, never the other way around.
 *
 * One row is written per Kafka event consumed from ANY of the 4 subscribed topics,
 * regardless of eventType. This table is effectively a durable log of everything the
 * global SSE feed has ever broadcast.
 */
@Entity
@Table(name = "notifications")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id")
    private String orderId;

    @Column(name = "source_topic", nullable = false)
    private String sourceTopic;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "message", nullable = false)
    private String message;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;
}
