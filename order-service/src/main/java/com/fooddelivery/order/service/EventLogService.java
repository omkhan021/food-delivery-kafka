package com.fooddelivery.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Handles the {@code event_log} upsert that underpins Kafka-replay visibility (see
 * ARCHITECTURE.md section 6).
 *
 * <p>This is deliberately implemented with a raw {@link JdbcTemplate} upsert rather than through
 * Spring Data JPA: we need {@code INSERT ... ON CONFLICT (topic, partition, kafka_offset) DO
 * UPDATE SET times_seen = times_seen + 1, last_seen_at = now() RETURNING times_seen} in a single
 * atomic statement, which JPA/Hibernate has no clean first-class API for. The returned
 * {@code times_seen} is exactly what the consumer uses to decide idempotently whether this is a
 * brand-new event (times_seen == 1, first delivery) or a replayed/redelivered one (times_seen &gt;
 * 1) — see {@link KafkaEventConsumerService}.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EventLogService {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
            INSERT INTO order_service.event_log
                (topic, partition, kafka_offset, event_key, event_type, order_id, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)
            ON CONFLICT (topic, partition, kafka_offset)
            DO UPDATE SET times_seen = event_log.times_seen + 1, last_seen_at = now()
            RETURNING times_seen
            """;

    /**
     * Upserts one event_log row for the given Kafka coordinates and returns the resulting
     * {@code times_seen}. A return value of {@code 1} means this is the first time this exact
     * (topic, partition, offset) has ever been seen by this service; any value &gt; 1 means it's a
     * re-delivery (either a consumer-group rebalance re-processing an uncommitted offset, or an
     * explicit replay triggered via {@code POST /api/admin/kafka/replay}).
     */
    public int upsertAndGetTimesSeen(String topic, int partition, long offset, String eventKey,
                                      String eventType, String orderId, String payloadJson) {
        Integer timesSeen = jdbcTemplate.queryForObject(
                UPSERT_SQL,
                Integer.class,
                topic, partition, offset, eventKey, eventType, orderId, payloadJson);
        return timesSeen == null ? 1 : timesSeen;
    }
}
