package com.saga.payment.events;

import java.time.Instant;
import java.util.UUID;

public record PaymentFailedEvent(
        String eventId,
        String orderId,
        String productId,     // Inventory needs this for compensation
        Integer quantity,     // Inventory needs this for compensation
        String reason,
        Instant occurredAt
) {
    public static PaymentFailedEvent from(StockReservedEvent event, String reason) {
        return new PaymentFailedEvent(
                UUID.randomUUID().toString(),
                event.orderId(),
                event.productId(),
                event.quantity(),
                reason,
                Instant.now()
        );
    }
}