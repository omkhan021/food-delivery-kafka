package com.fooddelivery.kitchen.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * JPA entity mapped to {@code kitchen_service.kitchen_orders} (created by Flyway
 * migration V1__init.sql - Hibernate only validates this mapping, it never creates
 * or alters the table, per {@code spring.jpa.hibernate.ddl-auto=validate}).
 *
 * <p>One row per {@code orderId}, tracking this service's local view of the kitchen
 * prep state machine: RECEIVED -> PREPARING -> PREPARED.
 */
@Entity
@Table(name = "kitchen_orders")
public class KitchenOrder {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private KitchenOrderStatus status;

    @Column(name = "estimated_minutes")
    private Integer estimatedMinutes;

    @Column(name = "received_at")
    private Instant receivedAt;

    @Column(name = "preparing_at")
    private Instant preparingAt;

    @Column(name = "prepared_at")
    private Instant preparedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected KitchenOrder() {
        // JPA
    }

    public KitchenOrder(String orderId, KitchenOrderStatus status, Instant receivedAt) {
        this.orderId = orderId;
        this.status = status;
        this.receivedAt = receivedAt;
    }

    @PrePersist
    protected void onCreate() {
        Instant now = Instant.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getOrderId() {
        return orderId;
    }

    public KitchenOrderStatus getStatus() {
        return status;
    }

    public void setStatus(KitchenOrderStatus status) {
        this.status = status;
    }

    public Integer getEstimatedMinutes() {
        return estimatedMinutes;
    }

    public void setEstimatedMinutes(Integer estimatedMinutes) {
        this.estimatedMinutes = estimatedMinutes;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Instant receivedAt) {
        this.receivedAt = receivedAt;
    }

    public Instant getPreparingAt() {
        return preparingAt;
    }

    public void setPreparingAt(Instant preparingAt) {
        this.preparingAt = preparingAt;
    }

    public Instant getPreparedAt() {
        return preparedAt;
    }

    public void setPreparedAt(Instant preparedAt) {
        this.preparedAt = preparedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
