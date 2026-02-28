package com.saga.payment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    // Topics this service PUBLISHES to
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

    // Topics this service CONSUMES from
    @Bean
    public NewTopic stockReservedTopic() {
        return TopicBuilder.name("stock.reserved")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Dead Letter Topics
    @Bean
    public NewTopic stockReservedDlt() {
        return TopicBuilder.name("stock.reserved.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }
}