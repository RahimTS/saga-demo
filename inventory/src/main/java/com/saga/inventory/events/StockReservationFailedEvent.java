package com.saga.inventory.events;

import java.time.Instant;
import java.util.UUID;

public record StockReservationFailedEvent(
        String eventId,
        String orderId,
        String reason,
        Instant occurredAt
) {
    public static StockReservationFailedEvent from(String orderId, String reason) {
        return new StockReservationFailedEvent(
                UUID.randomUUID().toString(),
                orderId,
                reason,
                Instant.now()
        );
    }
}