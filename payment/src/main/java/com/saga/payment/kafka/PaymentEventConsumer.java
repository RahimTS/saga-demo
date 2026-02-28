package com.saga.payment.kafka;

import com.saga.payment.events.StockReservedEvent;
import com.saga.payment.service.PaymentService;
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
public class PaymentEventConsumer {

    private final PaymentService paymentService;

    // ─────────────────────────────────────────────
    // Listen to stock.reserved → process payment
    // This is the only topic Payment Service consumes
    // ─────────────────────────────────────────────
    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = ".DLT"
    )
    @KafkaListener(topics = "stock.reserved", groupId = "payment-service-group")
    public void onStockReserved(StockReservedEvent event) {
        log.info("Received StockReservedEvent: eventId={} orderId={} amount={}",
                event.eventId(), event.orderId(), event.amount());
        paymentService.processPayment(event);
    }

    @DltHandler
    public void onStockReservedDlt(ConsumerRecord<String, StockReservedEvent> record) {
        log.error("StockReservedEvent sent to DLT: orderId={}", record.key());
        paymentService.saveDeadLetterEvent(
                record.topic(),
                record.value(),
                "Exhausted all retries"
        );
    }
}