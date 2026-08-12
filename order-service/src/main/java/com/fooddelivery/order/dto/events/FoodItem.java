package com.fooddelivery.order.dto.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

/** Nested type used inside {@link OrderEvent#getFoodItems()}. Per ARCHITECTURE.md section 4.1. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FoodItem {
    private String itemName;
    private int quantity;
    private BigDecimal price;
}
