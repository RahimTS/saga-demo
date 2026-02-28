package com.saga.order_service.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        String eventId,              // Unique ID for this event (idempotency key for consumers)
        String orderId,              // The order this event is about
        String customerId,
        String productId,
        Integer quantity,
        BigDecimal amount,
        boolean forcePaymentFailure, // Simulation flag
        boolean forceStockFailure,   // Simulation flag
        Instant occurredAt
) {
    // Factory method — clean way to create from an Order entity
    public static OrderCreatedEvent from(com.saga.order_service.entity.Order order) {
        return new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                order.getId().toString(),
                order.getCustomerId(),
                order.getProductId(),
                order.getQuantity(),
                order.getAmount(),
                order.isForcePaymentFailure(),
                order.isForceStockFailure(),
                Instant.now()
        );
    }
}