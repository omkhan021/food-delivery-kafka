package com.fooddelivery.order.dto.admin;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

/** Body of {@code POST /api/admin/kafka/replay}: {"listenerId": "order-status-group"}. */
@Getter
@Setter
public class ReplayRequest {
    @NotBlank
    private String listenerId;
}
