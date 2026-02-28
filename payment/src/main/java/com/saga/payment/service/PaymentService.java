package com.saga.payment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.payment.entity.*;
import com.saga.payment.events.*;
import com.saga.payment.kafka.PaymentEventPublisher;
import com.saga.payment.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final DeadLetterEventRepository deadLetterEventRepository;
    private final PaymentEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    // ─────────────────────────────────────────────
    // FORWARD PATH — Process payment
    // Triggered by: StockReservedEvent
    // Decision point of the entire saga
    // ─────────────────────────────────────────────
    @Transactional
    public void processPayment(StockReservedEvent event) {
        if (isAlreadyProcessed(event.eventId())) {
            log.warn("Duplicate event ignored: eventId={}", event.eventId());
            return;
        }

        // Simulate payment failure if flag is set
        if (event.forcePaymentFailure()) {
            log.warn("Force payment failure for orderId={}", event.orderId());
            handlePaymentFailure(event, "Forced payment failure");
            markAsProcessed(event.eventId());
            return;
        }

        // Simulate real payment logic here
        // In production: call payment gateway, validate card, etc.
        boolean paymentSucceeded = simulatePaymentGateway(event);

        if (paymentSucceeded) {
            handlePaymentSuccess(event);
        } else {
            handlePaymentFailure(event, "Payment gateway declined");
        }

        markAsProcessed(event.eventId());
    }

    // ─────────────────────────────────────────────
    // Happy path — save payment record, publish completed
    // ─────────────────────────────────────────────
    private void handlePaymentSuccess(StockReservedEvent event) {
        Payment payment = Payment.builder()
                .id(UUID.randomUUID())
                .orderId(event.orderId())
                .customerId(event.customerId())
                .amount(event.amount())
                .status(PaymentStatus.COMPLETED)
                .build();

        paymentRepository.save(payment);
        log.info("Payment COMPLETED for orderId={} paymentId={}", 
                event.orderId(), payment.getId());

        eventPublisher.publishPaymentCompleted(
                PaymentCompletedEvent.from(event.orderId(), payment.getId().toString())
        );
    }

    // ─────────────────────────────────────────────
    // Failure path — save failed record, publish failed
    // This triggers compensation in Inventory + Order
    // ─────────────────────────────────────────────
    private void handlePaymentFailure(StockReservedEvent event, String reason) {
        Payment payment = Payment.builder()
                .id(UUID.randomUUID())
                .orderId(event.orderId())
                .customerId(event.customerId())
                .amount(event.amount())
                .status(PaymentStatus.FAILED)
                .failureReason(reason)
                .build();

        paymentRepository.save(payment);
        log.warn("Payment FAILED for orderId={} reason={}", event.orderId(), reason);

        // Publishing this event kicks off the entire compensation chain:
        // Inventory releases stock → Order marks cancelled
        eventPublisher.publishPaymentFailed(
                PaymentFailedEvent.from(event, reason)
        );
    }

    // ─────────────────────────────────────────────
    // Simulated payment gateway
    // In a real system this calls Stripe, Razorpay, etc.
    // Returns false for amounts > 50000 to demo gateway decline
    // ─────────────────────────────────────────────
    private boolean simulatePaymentGateway(StockReservedEvent event) {
        // Simulate gateway decline for large amounts
        if (event.amount().doubleValue() > 50000) {
            log.warn("Payment gateway declined: amount {} exceeds limit", event.amount());
            return false;
        }
        return true;
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