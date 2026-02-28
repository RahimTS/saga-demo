package com.saga.payment.events;

import java.time.Instant;
import java.util.UUID;

public record PaymentCompletedEvent(
        String eventId,
        String orderId,
        String paymentId,
        Instant occurredAt
) {
    public static PaymentCompletedEvent from(String orderId, String paymentId) {
        return new PaymentCompletedEvent(
                UUID.randomUUID().toString(),
                orderId,
                paymentId,
                Instant.now()
        );
    }
}