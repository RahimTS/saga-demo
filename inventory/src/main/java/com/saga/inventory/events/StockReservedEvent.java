package com.saga.inventory.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record StockReservedEvent(
        String eventId,
        String orderId,
        String customerId,
        String productId,
        Integer quantity,
        BigDecimal amount,
        boolean forcePaymentFailure,  // forwarded from OrderCreatedEvent
        Instant occurredAt
) {
    public static StockReservedEvent from(OrderCreatedEvent order) {
        return new StockReservedEvent(
                UUID.randomUUID().toString(),
                order.orderId(),
                order.customerId(),
                order.productId(),
                order.quantity(),
                order.amount(),
                order.forcePaymentFailure(),
                Instant.now()
        );
    }
}