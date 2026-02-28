package com.saga.payment.events;

import java.math.BigDecimal;
import java.time.Instant;

public record StockReservedEvent(
        String eventId,
        String orderId,
        String customerId,
        String productId,     // forwarded in PaymentFailedEvent for inventory compensation
        Integer quantity,     // forwarded in PaymentFailedEvent for inventory compensation
        BigDecimal amount,
        boolean forcePaymentFailure,
        Instant occurredAt
) {
    
}