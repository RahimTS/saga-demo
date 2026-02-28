package com.saga.inventory.events;

import java.time.Instant;

public record PaymentFailedEvent(
        String eventId,
        String orderId,
        String productId,
        Integer quantity,
        String reason,
        Instant occurredAt
) {
    
}