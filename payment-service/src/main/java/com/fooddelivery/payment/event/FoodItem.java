package com.fooddelivery.payment.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;

/**
 * Mirrors the {@code FoodItem} nested type inside order-events' {@code OrderEvent} payload
 * (see ARCHITECTURE.md section 4.1). payment-service doesn't use this data for anything -
 * it's only here so OrderEvent deserializes the full contract shape without errors when
 * spring.json.use.type.headers=false forces Jackson to map the whole incoming JSON object.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FoodItem {

    private String itemName;
    private int quantity;
    private BigDecimal price;

    public FoodItem() {
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }
}
