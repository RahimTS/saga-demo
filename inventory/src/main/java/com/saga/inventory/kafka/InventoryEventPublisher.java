package com.saga.inventory.kafka;

import com.saga.inventory.events.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishStockReserved(StockReservedEvent event) {
        kafkaTemplate.send("stock.reserved", event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish StockReservedEvent orderId={}: {}",
                                event.orderId(), ex.getMessage());
                    } else {
                        log.info("Published StockReservedEvent: orderId={} partition={} offset={}",
                                event.orderId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }

    public void publishStockReservationFailed(StockReservationFailedEvent event) {
        kafkaTemplate.send("stock.reservation.failed", event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish StockReservationFailedEvent orderId={}: {}",
                                event.orderId(), ex.getMessage());
                    } else {
                        log.info("Published StockReservationFailedEvent: orderId={}", event.orderId());
                    }
                });
    }

    public void publishStockReleased(StockReleasedEvent event) {
        kafkaTemplate.send("stock.released", event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish StockReleasedEvent orderId={}: {}",
                                event.orderId(), ex.getMessage());
                    } else {
                        log.info("Published StockReleasedEvent (compensation complete): orderId={}",
                                event.orderId());
                    }
                });
    }
}