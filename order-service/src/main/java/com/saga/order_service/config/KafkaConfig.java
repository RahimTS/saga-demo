package com.saga.order_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    // Topics this service PUBLISHES to
    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name("order.created")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Topics this service CONSUMES from — still define them here
    // so they're created if they don't exist yet
    @Bean
    public NewTopic paymentCompletedTopic() {
        return TopicBuilder.name("payment.completed")
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

    @Bean
    public NewTopic stockReservationFailedTopic() {
        return TopicBuilder.name("stock.reservation.failed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Dead Letter Topics
    @Bean
    public NewTopic paymentCompletedDlt() {
        return TopicBuilder.name("payment.completed.DLT")
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

    @Bean
    public NewTopic stockReservationFailedDlt() {
        return TopicBuilder.name("stock.reservation.failed.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }
}