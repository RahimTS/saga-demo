package com.saga.payment.kafka;

import com.saga.payment.events.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishPaymentCompleted(PaymentCompletedEvent event) {
        kafkaTemplate.send("payment.completed", event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish PaymentCompletedEvent orderId={}: {}",
                                event.orderId(), ex.getMessage());
                    } else {
                        log.info("Published PaymentCompletedEvent: orderId={} partition={} offset={}",
                                event.orderId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }

    public void publishPaymentFailed(PaymentFailedEvent event) {
        kafkaTemplate.send("payment.failed", event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish PaymentFailedEvent orderId={}: {}",
                                event.orderId(), ex.getMessage());
                    } else {
                        log.info("Published PaymentFailedEvent: orderId={} — compensation chain started",
                                event.orderId());
                    }
                });
    }
}