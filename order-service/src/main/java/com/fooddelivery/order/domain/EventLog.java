package com.fooddelivery.order.domain;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * Read-only mapping of the {@code event_log} audit table (see ARCHITECTURE.md section 6).
 *
 * <p>Rows are actually written/updated via a raw upsert
 * ({@code INSERT ... ON CONFLICT (topic, partition, kafka_offset) DO UPDATE ...}) executed with
 * {@code JdbcTemplate} in {@code EventLogService}, because Spring Data JPA has no first-class
 * support for {@code ON CONFLICT ... DO UPDATE ... RETURNING}. This entity is used purely for
 * reading rows back out for the {@code GET /api/orders/{orderId}/event-log} endpoint.
 */
@Entity
@Table(name = "event_log")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class EventLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "topic", length = 64, nullable = false)
    private String topic;

    @Column(name = "partition", nullable = false)
    private int partition;

    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;

    @Column(name = "event_key", length = 64)
    private String eventKey;

    @Column(name = "event_type", length = 64, nullable = false)
    private String eventType;

    @Column(name = "order_id", length = 40)
    private String orderId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "times_seen", nullable = false)
    private int timesSeen;

    @Column(name = "first_seen_at", nullable = false)
    private Instant firstSeenAt;

    @Column(name = "last_seen_at", nullable = false)
    private Instant lastSeenAt;
}
