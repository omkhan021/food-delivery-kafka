package com.fooddelivery.order.service;

import com.fooddelivery.order.dto.admin.ConsumerGroupLagResponse;
import com.fooddelivery.order.dto.admin.ReplayResponse;
import com.fooddelivery.order.dto.admin.TopicInfoResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Backs the {@code /api/admin/kafka/...} endpoints (ARCHITECTURE.md section 8): topic/partition
 * introspection, consumer-group lag, and offset-reset-based replay. Talks to the broker directly
 * via the raw {@link AdminClient} bean (see {@code KafkaAdminClientConfig}) rather than through
 * Spring Kafka's higher-level abstractions, since none of {@code listOffsets},
 * {@code listConsumerGroupOffsets} or {@code alterConsumerGroupOffsets} have a Spring wrapper.
 */
@Service
@Slf4j
public class KafkaAdminService {

    /** The 4 project-wide topics (ARCHITECTURE.md section 3). */
    private static final List<String> ALL_TOPICS = List.of(
            "order-events", "payment-events", "kitchen-events", "delivery-events");

    /**
     * Consumer group id -&gt; topics it subscribes to, exactly per ARCHITECTURE.md section 3.
     * Used to know which topic-partitions to reset offsets for during a replay, and which
     * topic-partitions to compute lag against for a given group.
     */
    private static final Map<String, List<String>> GROUP_TOPICS = Map.of(
            "payment-service-group", List.of("order-events"),
            "kitchen-service-group", List.of("payment-events"),
            "delivery-service-group", List.of("kitchen-events"),
            "order-status-group", List.of("payment-events", "kitchen-events", "delivery-events"),
            "notification-service-group", List.of("order-events", "payment-events", "kitchen-events", "delivery-events")
    );

    private final AdminClient adminClient;
    private final KafkaListenerEndpointRegistry listenerEndpointRegistry;

    public KafkaAdminService(AdminClient adminClient, KafkaListenerEndpointRegistry listenerEndpointRegistry) {
        this.adminClient = adminClient;
        this.listenerEndpointRegistry = listenerEndpointRegistry;
    }

    public List<TopicInfoResponse> listTopics() {
        try {
            Map<String, TopicDescription> descriptions = adminClient.describeTopics(ALL_TOPICS).allTopicNames().get();

            List<TopicPartition> allPartitions = new ArrayList<>();
            descriptions.forEach((topic, desc) -> desc.partitions()
                    .forEach(p -> allPartitions.add(new TopicPartition(topic, p.partition()))));

            Map<TopicPartition, Long> endOffsets = endOffsets(allPartitions);

            List<TopicInfoResponse> result = new ArrayList<>();
            for (String topic : ALL_TOPICS) {
                TopicDescription desc = descriptions.get(topic);
                if (desc == null) {
                    continue;
                }
                List<TopicInfoResponse.PartitionOffset> partitions = desc.partitions().stream()
                        .map(p -> TopicInfoResponse.PartitionOffset.builder()
                                .partition(p.partition())
                                .endOffset(endOffsets.getOrDefault(new TopicPartition(topic, p.partition()), 0L))
                                .build())
                        .collect(Collectors.toList());
                result.add(TopicInfoResponse.builder()
                        .topic(topic)
                        .partitionCount(desc.partitions().size())
                        .partitions(partitions)
                        .build());
            }
            return result;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to list Kafka topics: " + ex.getMessage(), ex);
        }
    }

    public List<ConsumerGroupLagResponse> listConsumerGroups() {
        try {
            List<String> groupIds = new ArrayList<>(GROUP_TOPICS.keySet());
            Map<String, ConsumerGroupDescription> descriptions =
                    adminClient.describeConsumerGroups(groupIds).all().get();

            List<ConsumerGroupLagResponse> result = new ArrayList<>();
            for (String groupId : groupIds) {
                result.add(buildGroupLag(groupId, descriptions.get(groupId)));
            }
            return result;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to list consumer groups: " + ex.getMessage(), ex);
        }
    }

    private ConsumerGroupLagResponse buildGroupLag(String groupId, ConsumerGroupDescription description) throws Exception {
        List<String> topics = GROUP_TOPICS.get(groupId);

        Map<TopicPartition, OffsetAndMetadata> committed;
        try {
            committed = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        } catch (Exception ex) {
            log.warn("Could not fetch committed offsets for group {}: {}", groupId, ex.getMessage());
            committed = Map.of();
        }

        Map<String, TopicDescription> descriptions = adminClient.describeTopics(topics).allTopicNames().get();
        List<TopicPartition> allPartitions = new ArrayList<>();
        descriptions.forEach((topic, desc) -> desc.partitions()
                .forEach(p -> allPartitions.add(new TopicPartition(topic, p.partition()))));
        Map<TopicPartition, Long> endOffsets = endOffsets(allPartitions);

        List<ConsumerGroupLagResponse.PartitionLag> lags = new ArrayList<>();
        long totalLag = 0;
        for (TopicPartition tp : allPartitions) {
            long endOffset = endOffsets.getOrDefault(tp, 0L);
            OffsetAndMetadata committedOam = committed.get(tp);
            Long committedOffset = committedOam != null ? committedOam.offset() : null;
            long lag = Math.max(0L, endOffset - (committedOffset != null ? committedOffset : 0L));
            totalLag += lag;
            lags.add(ConsumerGroupLagResponse.PartitionLag.builder()
                    .topic(tp.topic())
                    .partition(tp.partition())
                    .committedOffset(committedOffset)
                    .endOffset(endOffset)
                    .lag(lag)
                    .build());
        }

        String state = description != null ? description.state().toString() : "UNKNOWN";
        return ConsumerGroupLagResponse.builder()
                .groupId(groupId)
                .state(state)
                .partitionLags(lags)
                .totalLag(totalLag)
                .build();
    }

    /**
     * Resets committed offsets for {@code listenerId}'s subscribed topics back to "earliest",
     * which is what makes replay visible via {@code event_log.times_seen} incrementing on
     * re-delivery.
     *
     * <p><b>Operational note</b> (documented per ARCHITECTURE.md section 8): Kafka's
     * {@code alterConsumerGroupOffsets} only succeeds when the target group has no active
     * members (state {@code EMPTY}). For {@code order-status-group} (this service's own
     * listener) we pause the live listener container via
     * {@link KafkaListenerEndpointRegistry} before altering offsets and resume it after, so the
     * live consumer doesn't race the reset. For the other 4 groups (owned by the sibling
     * services), this service has no control over their processes -- for a clean replay, briefly
     * stop that service first (or simply restart it afterward so it picks up the reset offset on
     * its next poll). That is normal, correct Kafka operational practice, not a bug.
     */
    public ReplayResponse replay(String listenerId) {
        if (!GROUP_TOPICS.containsKey(listenerId)) {
            throw new IllegalArgumentException("Unknown listenerId '" + listenerId + "'. Must be one of: " + GROUP_TOPICS.keySet());
        }

        MessageListenerContainer ownContainer = "order-status-group".equals(listenerId)
                ? listenerEndpointRegistry.getListenerContainer("order-status-group")
                : null;

        try {
            if (ownContainer != null) {
                ownContainer.pause();
                log.info("Paused order-status-group listener container ahead of replay");
                // Give the container a brief moment to actually stop polling before we alter offsets.
                Thread.sleep(Duration.ofMillis(500).toMillis());
            }

            List<String> topics = GROUP_TOPICS.get(listenerId);
            Map<String, TopicDescription> descriptions = adminClient.describeTopics(topics).allTopicNames().get();
            List<TopicPartition> allPartitions = new ArrayList<>();
            descriptions.forEach((topic, desc) -> desc.partitions()
                    .forEach(p -> allPartitions.add(new TopicPartition(topic, p.partition()))));

            Map<TopicPartition, OffsetSpec> earliestSpecs = allPartitions.stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestResults =
                    adminClient.listOffsets(earliestSpecs).all().get();

            Map<TopicPartition, OffsetAndMetadata> resetTargets = new LinkedHashMap<>();
            List<ReplayResponse.ResetPartition> resetPartitions = new ArrayList<>();
            for (TopicPartition tp : allPartitions) {
                long earliestOffset = earliestResults.get(tp).offset();
                resetTargets.put(tp, new OffsetAndMetadata(earliestOffset));
                resetPartitions.add(ReplayResponse.ResetPartition.builder()
                        .topic(tp.topic())
                        .partition(tp.partition())
                        .resetToOffset(earliestOffset)
                        .build());
            }

            adminClient.alterConsumerGroupOffsets(listenerId, resetTargets).all().get();

            String message = "order-status-group".equals(listenerId)
                    ? "Offsets reset to earliest and listener container resumed. Replay will be processed live; watch times_seen increment via GET /api/orders/{orderId}/event-log."
                    : "Offsets reset to earliest for group '" + listenerId + "'. This service does not control that consumer's process -- "
                        + "restart " + ownerServiceHint(listenerId) + " (or ensure it is briefly stopped during this call) to pick up the replay.";

            return ReplayResponse.builder()
                    .listenerId(listenerId)
                    .resetPartitions(resetPartitions)
                    .message(message)
                    .build();
        } catch (Exception ex) {
            throw new RuntimeException("Replay failed for listenerId '" + listenerId + "': " + ex.getMessage(), ex);
        } finally {
            if (ownContainer != null) {
                ownContainer.resume();
                log.info("Resumed order-status-group listener container after replay");
            }
        }
    }

    private String ownerServiceHint(String groupId) {
        return switch (groupId) {
            case "payment-service-group" -> "payment-service";
            case "kitchen-service-group" -> "kitchen-service";
            case "delivery-service-group" -> "delivery-service";
            case "notification-service-group" -> "notification-service";
            default -> "the owning service";
        };
    }

    private Map<TopicPartition, Long> endOffsets(List<TopicPartition> partitions) throws Exception {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> latestSpecs = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> results =
                adminClient.listOffsets(latestSpecs).all().get();
        Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
        results.forEach((tp, info) -> offsets.put(tp, info.offset()));
        return offsets;
    }
}
