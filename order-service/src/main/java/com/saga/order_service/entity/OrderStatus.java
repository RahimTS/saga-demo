package com.saga.order_service.entity;

public enum OrderStatus {
    PENDING,        // Just created, saga in progress
    COMPLETED,      // Payment succeeded
    CANCELLED       // Payment failed or stock unavailable
}
