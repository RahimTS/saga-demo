package com.saga.order_service.events;

import java.time.Instant;

public record StockReservationFailedEvent(
        String eventId,
        String orderId,
        String reason,
        Instant occurredAt
) {
}
