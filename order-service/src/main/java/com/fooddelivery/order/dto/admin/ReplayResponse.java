package com.fooddelivery.order.dto.admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/** Response of {@code POST /api/admin/kafka/replay}. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ReplayResponse {
    private String listenerId;
    private List<ResetPartition> resetPartitions;
    private String message;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class ResetPartition {
        private String topic;
        private int partition;
        private long resetToOffset;
    }
}
