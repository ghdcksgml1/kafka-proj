package com.practice.kafka.consumer;

import java.time.LocalDateTime;

public class OrderDto {
    private final String orderId;
    private final String shopId;
    private final String menuName;
    private final String userName;
    private final String phoneNumber;
    private final String address;
    private final LocalDateTime orderTime;

    public OrderDto(String orderId, String shopId, String menuName, String userName, String phoneNumber, String address, LocalDateTime orderTime) {
        this.orderId = orderId;
        this.shopId = shopId;
        this.menuName = menuName;
        this.userName = userName;
        this.phoneNumber = phoneNumber;
        this.address = address;
        this.orderTime = orderTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getShopId() {
        return shopId;
    }

    public String getMenuName() {
        return menuName;
    }

    public String getUserName() {
        return userName;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public String getAddress() {
        return address;
    }

    public LocalDateTime getOrderTime() {
        return orderTime;
    }
}
