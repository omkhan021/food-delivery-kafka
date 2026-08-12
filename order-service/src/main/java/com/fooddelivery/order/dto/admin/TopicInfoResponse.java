package com.fooddelivery.order.dto.admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/** Response item for {@code GET /api/admin/kafka/topics}. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TopicInfoResponse {
    private String topic;
    private int partitionCount;
    private List<PartitionOffset> partitions;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class PartitionOffset {
        private int partition;
        private long endOffset;
    }
}
