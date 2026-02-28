package com.saga.payment.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;

@Entity
@Table(name = "processed_events", schema = "payment_schema")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessedEvent {

    @Id
    @Column(name = "event_id")
    private String eventId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
