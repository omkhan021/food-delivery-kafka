package com.fooddelivery.order.dto.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderResponse {
    private String orderId;
    private String userId;
    private BigDecimal paymentAmount;
    private String status;
    private LocalDate orderDate;
    private LocalTime orderTime;
    private Instant createdAt;
    private Instant updatedAt;
    private List<OrderItemResponse> items;
    /** Only populated by GET /api/orders/{orderId} (not the list endpoint). */
    private List<OrderStatusHistoryResponse> statusHistory;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OrderItemResponse {
        private String itemName;
        private int quantity;
        private BigDecimal price;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OrderStatusHistoryResponse {
        private String status;
        private String sourceTopic;
        private String note;
        private Instant createdAt;
    }
}
