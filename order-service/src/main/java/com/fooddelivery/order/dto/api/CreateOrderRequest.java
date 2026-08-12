package com.fooddelivery.order.dto.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

/** Body of {@code POST /api/orders} (ARCHITECTURE.md section 8). */
@Getter
@Setter
public class CreateOrderRequest {

    @NotBlank
    private String userId;

    @NotEmpty
    @Valid
    private List<FoodItemRequest> foodItems;

    /**
     * Optional. Per ARCHITECTURE.md section 8, paymentAmount is "computed server-side as sum of
     * qty*price if not supplied" -- so a client MAY pass it explicitly, but the normal/expected
     * path (and what the frontend does) is to omit it and let order-service compute it from
     * {@link #foodItems}.
     */
    private BigDecimal paymentAmount;

    /** Optional; defaults to today if omitted. */
    private LocalDate orderDate;

    /** Optional; defaults to now if omitted. */
    private LocalTime orderTime;

    @Getter
    @Setter
    public static class FoodItemRequest {
        @NotBlank
        private String itemName;

        private int quantity;

        /** Optional per-request; paymentAmount is always (re)computed server-side from these. */
        private java.math.BigDecimal price;
    }
}
