package com.saga.inventory.kafka;

import com.saga.inventory.events.*;
import com.saga.inventory.service.InventoryService;

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
public class InventoryEventConsumer {

    private final InventoryService inventoryService;

    // ─────────────────────────────────────────────
    // FORWARD PATH
    // Listen to order.created → reserve stock
    // ─────────────────────────────────────────────
    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = ".DLT"
    )
    @KafkaListener(topics = "order.created", groupId = "inventory-service-group")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent: eventId={} orderId={} productId={}",
                event.eventId(), event.orderId(), event.productId());
        inventoryService.reserveStock(event);
    }

    @DltHandler
    public void onOrderCreatedDlt(ConsumerRecord<String, OrderCreatedEvent> record) {
        log.error("OrderCreatedEvent sent to DLT: orderId={}", record.key());
        inventoryService.saveDeadLetterEvent(
                record.topic(),
                record.value(),
                "Exhausted all retries"
        );
    }

    // ─────────────────────────────────────────────
    // COMPENSATION PATH
    // Listen to payment.failed → release stock
    // ─────────────────────────────────────────────
    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = ".DLT"
    )
    @KafkaListener(topics = "payment.failed", groupId = "inventory-service-group")
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.info("Received PaymentFailedEvent (compensation): eventId={} orderId={}",
                event.eventId(), event.orderId());
        inventoryService.releaseStock(event);
    }

    @DltHandler
    public void onPaymentFailedDlt(ConsumerRecord<String, PaymentFailedEvent> record) {
        log.error("PaymentFailedEvent sent to DLT: orderId={}", record.key());
        inventoryService.saveDeadLetterEvent(
                record.topic(),
                record.value(),
                "Exhausted all retries"
        );
    }
}