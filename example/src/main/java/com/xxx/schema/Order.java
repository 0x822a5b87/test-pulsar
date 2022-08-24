package com.xxx.schema;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author 0x822a5b87
 */
public class Order {
    private String orderId;
    private String orderName;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderName() {
        return orderName;
    }

    public void setOrderName(String orderName) {
        this.orderName = orderName;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("orderId", orderId)
                .append("orderName", orderName)
                .toString();
    }
}
