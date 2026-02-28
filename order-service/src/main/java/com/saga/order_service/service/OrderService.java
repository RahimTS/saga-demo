package com.saga.order_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.order_service.dto.CreateOrderRequest;
import com.saga.order_service.entity.*;
import com.saga.order_service.events.*;
import com.saga.order_service.kafka.OrderEventPublisher;
import com.saga.order_service.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final DeadLetterEventRepository deadLetterEventRepository;
    private final OrderEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    // ─────────────────────────────────────────────
    // Called by REST controller
    // ─────────────────────────────────────────────
    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        Order order = Order.builder()
                .id(UUID.randomUUID())
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .amount(request.getAmount())
                .status(OrderStatus.PENDING)
                .forcePaymentFailure(request.isForcePaymentFailure())
                .forceStockFailure(request.isForceStockFailure())
                .build();

        orderRepository.save(order);
        log.info("Order created with id={} status=PENDING", order.getId());

        // Publish event AFTER saving — if Kafka fails, transaction rolls back
        OrderCreatedEvent event = OrderCreatedEvent.from(order);
        eventPublisher.publishOrderCreated(event);

        return order;
    }

    // ─────────────────────────────────────────────
    // Called by Kafka consumer — PaymentCompleted
    // ─────────────────────────────────────────────
    @Transactional
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        // Idempotency check — have we seen this event before?
        if (isAlreadyProcessed(event.eventId())) {
            log.warn("Duplicate event ignored: eventId={}", event.eventId());
            return;
        }

        orderRepository.findById(UUID.fromString(event.orderId()))
                .ifPresentOrElse(order -> {
                    order.setStatus(OrderStatus.COMPLETED);
                    orderRepository.save(order);
                    log.info("Order COMPLETED: orderId={}", event.orderId());
                }, () -> log.error("Order not found for orderId={}", event.orderId()));

        markAsProcessed(event.eventId());
    }

    // ─────────────────────────────────────────────
    // Called by Kafka consumer — PaymentFailed
    // ─────────────────────────────────────────────
    @Transactional
    public void handlePaymentFailed(PaymentFailedEvent event) {
        if (isAlreadyProcessed(event.eventId())) {
            log.warn("Duplicate event ignored: eventId={}", event.eventId());
            return;
        }

        orderRepository.findById(UUID.fromString(event.orderId()))
                .ifPresentOrElse(order -> {
                    order.setStatus(OrderStatus.CANCELLED);
                    orderRepository.save(order);
                    log.info("Order CANCELLED due to payment failure: orderId={} reason={}", 
                             event.orderId(), event.reason());
                }, () -> log.error("Order not found for orderId={}", event.orderId()));

        markAsProcessed(event.eventId());
    }

    // ─────────────────────────────────────────────
    // Called by Kafka consumer — StockReservationFailed
    // ─────────────────────────────────────────────
    @Transactional
    public void handleStockReservationFailed(StockReservationFailedEvent event) {
        if (isAlreadyProcessed(event.eventId())) {
            log.warn("Duplicate event ignored: eventId={}", event.eventId());
            return;
        }

        orderRepository.findById(UUID.fromString(event.orderId()))
                .ifPresentOrElse(order -> {
                    order.setStatus(OrderStatus.CANCELLED);
                    orderRepository.save(order);
                    log.info("Order CANCELLED due to stock failure: orderId={} reason={}",
                             event.orderId(), event.reason());
                }, () -> log.error("Order not found for orderId={}", event.orderId()));

        markAsProcessed(event.eventId());
    }

    // ─────────────────────────────────────────────
    // Called by DLT handler in consumer
    // ─────────────────────────────────────────────
    @Transactional
    public void saveDeadLetterEvent(String topic, Object payload, String errorMessage) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            DeadLetterEvent dle = DeadLetterEvent.builder()
                    .topic(topic)
                    .payload(json)
                    .errorMessage(errorMessage)
                    .failedAt(Instant.now())
                    .build();
            deadLetterEventRepository.save(dle);
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
        processedEventRepository.save(
                ProcessedEvent.builder()
                        .eventId(eventId)
                        .processedAt(Instant.now())
                        .build()
        );
    }
}
