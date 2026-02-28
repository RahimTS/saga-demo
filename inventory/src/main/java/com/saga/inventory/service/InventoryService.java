package com.saga.inventory.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.inventory.entity.*;
import com.saga.inventory.events.*;
import com.saga.inventory.kafka.InventoryEventPublisher;
import com.saga.inventory.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final InventoryRepository inventoryRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final DeadLetterEventRepository deadLetterEventRepository;
    private final InventoryEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    // ─────────────────────────────────────────────
    // FORWARD PATH — Reserve stock for an order
    // Triggered by: OrderCreatedEvent
    // ─────────────────────────────────────────────
    @Transactional
    public void reserveStock(OrderCreatedEvent event) {
        if (isAlreadyProcessed(event.eventId())) {
            log.warn("Duplicate event ignored: eventId={}", event.eventId());
            return;
        }

        // Simulate stock failure if flag is set
        if (event.forceStockFailure()) {
            log.warn("Force stock failure for orderId={}", event.orderId());
            eventPublisher.publishStockReservationFailed(
                    StockReservationFailedEvent.from(event.orderId(), "Forced stock failure")
            );
            markAsProcessed(event.eventId());
            return;
        }

        Inventory inventory = inventoryRepository.findByProductId(event.productId())
                .orElseThrow(() -> new RuntimeException(
                        "Product not found: " + event.productId()));

        // Check available stock
        if (inventory.getAvailableQuantity() < event.quantity()) {
            log.warn("Insufficient stock for productId={} available={} requested={}",
                    event.productId(), inventory.getAvailableQuantity(), event.quantity());

            eventPublisher.publishStockReservationFailed(
                    StockReservationFailedEvent.from(event.orderId(), "Insufficient stock")
            );
            markAsProcessed(event.eventId());
            return;
        }

        // Reserve the stock — increment reserved, don't touch quantity yet
        inventory.setReserved(inventory.getReserved() + event.quantity());
        inventoryRepository.save(inventory);

        log.info("Stock reserved for orderId={} productId={} quantity={}",
                event.orderId(), event.productId(), event.quantity());

        eventPublisher.publishStockReserved(StockReservedEvent.from(event));
        markAsProcessed(event.eventId());
    }

    // ─────────────────────────────────────────────
    // COMPENSATION PATH — Release stock on payment failure
    // Triggered by: PaymentFailedEvent
    // ─────────────────────────────────────────────
    @Transactional
    public void releaseStock(PaymentFailedEvent event) {
        if (isAlreadyProcessed(event.eventId())) {
            log.warn("Duplicate event ignored: eventId={}", event.eventId());
            return;
        }

        inventoryRepository.findByProductId(event.productId())
                .ifPresentOrElse(inventory -> {
                    // Decrement reserved — stock is free again
                    int newReserved = Math.max(0, inventory.getReserved() - event.quantity());
                    inventory.setReserved(newReserved);
                    inventoryRepository.save(inventory);

                    log.info("Stock released (compensation) for orderId={} productId={} quantity={}",
                            event.orderId(), event.productId(), event.quantity());

                    eventPublisher.publishStockReleased(StockReleasedEvent.from(event));

                }, () -> log.error("Product not found during compensation: productId={}",
                        event.productId()));

        markAsProcessed(event.eventId());
    }

    // ─────────────────────────────────────────────
    // DLT persistence
    // ─────────────────────────────────────────────
    @Transactional
    public void saveDeadLetterEvent(String topic, Object payload, String errorMessage) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            deadLetterEventRepository.save(DeadLetterEvent.builder()
                    .topic(topic)
                    .payload(json)
                    .errorMessage(errorMessage)
                    .failedAt(Instant.now())
                    .build());
            log.error("Dead letter event saved: topic={} error={}", topic, errorMessage);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize dead letter payload", e);
        }
    }

    // ─────────────────────────────────────────────
    // Idempotency helpers
    // ─────────────────────────────────────────────
    private boolean isAlreadyProcessed(String eventId) {
        return processedEventRepository.existsById(eventId);
    }

    private void markAsProcessed(String eventId) {
        processedEventRepository.save(ProcessedEvent.builder()
                .eventId(eventId)
                .processedAt(Instant.now())
                .build());
    }
}