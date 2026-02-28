package com.saga.inventory.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    // Topics this service PUBLISHES to
    @Bean
    public NewTopic stockReservedTopic() {
        return TopicBuilder.name("stock.reserved")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic stockReservationFailedTopic() {
        return TopicBuilder.name("stock.reservation.failed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic stockReleasedTopic() {
        return TopicBuilder.name("stock.released")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Topics this service CONSUMES from
    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name("order.created")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentFailedTopic() {
        return TopicBuilder.name("payment.failed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Dead Letter Topics
    @Bean
    public NewTopic orderCreatedDlt() {
        return TopicBuilder.name("order.created.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentFailedDlt() {
        return TopicBuilder.name("payment.failed.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }
}