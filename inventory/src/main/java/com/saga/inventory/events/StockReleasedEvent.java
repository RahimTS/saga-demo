package com.saga.inventory.events;

import java.time.Instant;
import java.util.UUID;

public record StockReleasedEvent(
        String eventId,
        String orderId,
        String productId,
        Integer quantity,
        Instant occurredAt
) {
    public static StockReleasedEvent from(PaymentFailedEvent event) {
        return new StockReleasedEvent(
                UUID.randomUUID().toString(),
                event.orderId(),
                event.productId(),
                event.quantity(),
                Instant.now()
        );
    }
}