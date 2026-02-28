package com.saga.order_service.kafka;

import com.saga.order_service.events.*;
import com.saga.order_service.service.OrderService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {

    private final OrderService orderService;

    // ─────────────────────────────────────────────
    // Listen to payment.completed
    // Retry 3 times: 1s → 2s → 4s, then → DLT
    // ─────────────────────────────────────────────
    @RetryableTopic(
            attempts = "4",  // 1 original + 3 retries
            backOff = @BackOff(delay = 1000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = ".DLT"
    )
    @KafkaListener(topics = "payment.completed", groupId = "order-service-group",
                   containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        log.info("Received PaymentCompletedEvent: eventId={} orderId={}", 
                 event.eventId(), event.orderId());
        orderService.handlePaymentCompleted(event);
    }

    @DltHandler
    public void onPaymentCompletedDlt(ConsumerRecord<String, PaymentCompletedEvent> record) {
        log.error("PaymentCompletedEvent sent to DLT: key={}", record.key());
        orderService.saveDeadLetterEvent(
                record.topic(),
                record.value(),
                "Exhausted all retries"
        );
    }

    // ─────────────────────────────────────────────
    // Listen to payment.failed
    // ─────────────────────────────────────────────
    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = ".DLT"
    )
    @KafkaListener(topics = "payment.failed", groupId = "order-service-group",
                   containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.info("Received PaymentFailedEvent: eventId={} orderId={} reason={}",
                event.eventId(), event.orderId(), event.reason());
        orderService.handlePaymentFailed(event);
    }

    @DltHandler
    public void onPaymentFailedDlt(ConsumerRecord<String, PaymentFailedEvent> record) {
        log.error("PaymentFailedEvent sent to DLT: key={}", record.key());
        orderService.saveDeadLetterEvent(
                record.topic(),
                record.value(),
                "Exhausted all retries"
        );
    }

    // ─────────────────────────────────────────────
    // Listen to stock.reservation.failed
    // ─────────────────────────────────────────────
    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = ".DLT"
    )
    @KafkaListener(topics = "stock.reservation.failed", groupId = "order-service-group",
                   containerFactory = "kafkaListenerContainerFactory")
    public void onStockReservationFailed(StockReservationFailedEvent event) {
        log.info("Received StockReservationFailedEvent: eventId={} orderId={} reason={}",
                event.eventId(), event.orderId(), event.reason());
        orderService.handleStockReservationFailed(event);
    }

    @DltHandler
    public void onStockReservationFailedDlt(ConsumerRecord<String, StockReservationFailedEvent> record) {
        log.error("StockReservationFailedEvent sent to DLT: key={}", record.key());
        orderService.saveDeadLetterEvent(
                record.topic(),
                record.value(),
                "Exhausted all retries"
        );
    }
}