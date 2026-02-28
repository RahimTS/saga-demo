package com.saga.order_service.events;

import java.time.Instant;

public record PaymentCompletedEvent(
        String eventId,
        String orderId,
        String paymentId,
        Instant occurredAt
) {}