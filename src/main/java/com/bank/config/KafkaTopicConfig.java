package com.bank.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topics.transaction-initiated}")
    private String transactionInitiated;

    @Value("${kafka.topics.fraud-check-result}")
    private String fraudCheckResult;

    @Value("${kafka.topics.balance-update}")
    private String balanceUpdate;

    @Value("${kafka.topics.notification}")
    private String notification;

    @Value("${kafka.topics.dead-letter}")
    private String deadLetter;

    @Bean
    public NewTopic transactionInitiatedTopic() {
        return TopicBuilder.name(transactionInitiated)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic fraudCheckResultTopic() {
        return TopicBuilder.name(fraudCheckResult)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic balanceUpdateTopic() {
        return TopicBuilder.name(balanceUpdate)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationTopic() {
        return TopicBuilder.name(notification)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(deadLetter)
                .partitions(1)
                .replicas(1)
                .build();
    }
}