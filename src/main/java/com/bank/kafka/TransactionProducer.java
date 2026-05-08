package com.bank.kafka;

import com.bank.dto.TransactionEvents.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.transaction-initiated}")
    private String transactionInitiatedTopic;

    @Value("${kafka.topics.fraud-check-result}")
    private String fraudCheckResultTopic;

    @Value("${kafka.topics.balance-update}")
    private String balanceUpdateTopic;

    @Value("${kafka.topics.notification}")
    private String notificationTopic;

    // ─── Publish: Transaction Initiated ───────────────────────────────────────
    public void publishTransactionInitiated(TransactionInitiatedEvent event) {
        String key = event.getTransactionId().toString();
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(transactionInitiatedTopic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("✅ Transaction initiated published: id={}, partition={}, offset={}",
                        event.getTransactionId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("❌ Failed to publish transaction initiated: id={}",
                        event.getTransactionId(), ex);
            }
        });
    }

    // ─── Publish: Fraud Check Result ──────────────────────────────────────────
    public void publishFraudCheckResult(FraudCheckResultEvent event) {
        String key = event.getTransactionId().toString();
        kafkaTemplate.send(fraudCheckResultTopic, key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("✅ Fraud check result published: id={}, fraudulent={}",
                                event.getTransactionId(), event.isFraudulent());
                    } else {
                        log.error("❌ Failed to publish fraud check result: id={}",
                                event.getTransactionId(), ex);
                    }
                });
    }

    // ─── Publish: Balance Update ──────────────────────────────────────────────
    public void publishBalanceUpdate(BalanceUpdateEvent event) {
        String key = event.getTransactionId().toString();
        kafkaTemplate.send(balanceUpdateTopic, key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("✅ Balance update published: id={}",
                                event.getTransactionId());
                    } else {
                        log.error("❌ Failed to publish balance update: id={}",
                                event.getTransactionId(), ex);
                    }
                });
    }

    // ─── Publish: Notification ────────────────────────────────────────────────
    public void publishNotification(TransactionNotificationEvent event) {
        String key = event.getAccountId();
        kafkaTemplate.send(notificationTopic, key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("✅ Notification published: accountId={}, status={}",
                                event.getAccountId(), event.getStatus());
                    } else {
                        log.error("❌ Failed to publish notification: accountId={}",
                                event.getAccountId(), ex);
                    }
                });
    }
}