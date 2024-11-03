package com.practice.kafka.consumer;

import java.sql.*;
import java.util.List;

public class OrderDBHandler {
    private final Connection conn;
    private static final String INSERT_ORDER_SQL = "INSERT INTO public.orders " +
            "(ord_id, shop_id, menu_name, user_name, phone_number, address, order_time) "+
            "values (?, ?, ?, ?, ?, ?, ?)";

    public OrderDBHandler(String url, String user, String password) {
        try {
            this.conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertOrder(OrderDto orderDto) {
        try {
            PreparedStatement pstmt = this.conn.prepareStatement(INSERT_ORDER_SQL);
            pstmt.setString(1, orderDto.getOrderId());
            pstmt.setString(2, orderDto.getShopId());
            pstmt.setString(3, orderDto.getMenuName());
            pstmt.setString(4, orderDto.getUserName());
            pstmt.setString(5, orderDto.getPhoneNumber());
            pstmt.setString(6, orderDto.getAddress());
            pstmt.setTimestamp(7, Timestamp.valueOf(orderDto.getOrderTime()));

            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertOrders(List<OrderDto> orderDtoList) {
        try {
            PreparedStatement pstmt = this.conn.prepareStatement(INSERT_ORDER_SQL);

            for (var orderDto : orderDtoList) {
                pstmt.setString(1, orderDto.getOrderId());
                pstmt.setString(2, orderDto.getShopId());
                pstmt.setString(3, orderDto.getMenuName());
                pstmt.setString(4, orderDto.getUserName());
                pstmt.setString(5, orderDto.getPhoneNumber());
                pstmt.setString(6, orderDto.getAddress());
                pstmt.setTimestamp(7, Timestamp.valueOf(orderDto.getOrderTime()));

                pstmt.addBatch();
            }
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
