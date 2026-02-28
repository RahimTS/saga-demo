package com.saga.inventory.events;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String eventId,
        String orderId,
        String customerId,
        String productId,
        Integer quantity,
        BigDecimal amount,
        boolean forcePaymentFailure,  // passed through to StockReservedEvent
        boolean forceStockFailure,    // used here to simulate stock failure
        Instant occurredAt
) {}