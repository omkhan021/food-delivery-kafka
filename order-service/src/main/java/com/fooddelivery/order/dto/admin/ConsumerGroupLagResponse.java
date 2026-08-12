package com.fooddelivery.order.dto.admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/** Response item for {@code GET /api/admin/kafka/consumer-groups}. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConsumerGroupLagResponse {
    private String groupId;
    private String state;
    private List<PartitionLag> partitionLags;
    private long totalLag;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class PartitionLag {
        private String topic;
        private int partition;
        private Long committedOffset;
        private long endOffset;
        private long lag;
    }
}
