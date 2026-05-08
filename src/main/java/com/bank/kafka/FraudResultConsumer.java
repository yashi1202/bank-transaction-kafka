package com.bank.kafka;

import com.bank.dto.TransactionEvents.*;
import com.bank.entity.Transaction;
import com.bank.repository.AccountRepository;
import com.bank.repository.TransactionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Fraud Result Processor
 * Consumes: fraud-check-result
 * Produces: balance-update (if approved) OR transaction-notification (if rejected)
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class FraudResultConsumer {

    private final TransactionRepository transactionRepository;
    private final AccountRepository accountRepository;
    private final TransactionProducer transactionProducer;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${kafka.topics.fraud-check-result}",
            groupId = "fraud-result-processor-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeFraudCheckResult(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset) {

        FraudCheckResultEvent event = objectMapper.convertValue(payload, FraudCheckResultEvent.class);
        log.info("Processing fraud result: transactionId={}, fraudulent={}, riskScore={}",
                event.getTransactionId(), event.isFraudulent(), event.getRiskScore());

        Transaction transaction = transactionRepository.findById(event.getTransactionId())
                .orElseThrow(() -> new RuntimeException("Transaction not found: " + event.getTransactionId()));

        transaction.setFraudCheckResult(event.getReason());

        if (event.isFraudulent()) {
            // Reject the transaction
            transaction.setStatus(Transaction.TransactionStatus.REJECTED);
            transactionRepository.save(transaction);
            log.warn("Transaction REJECTED due to fraud: transactionId={}, reason={}",
                    event.getTransactionId(), event.getReason());

            // Find account owner name and send notification
            accountRepository.findById(event.getFromAccountId()).ifPresent(account -> {
                transactionProducer.publishNotification(TransactionNotificationEvent.builder()
                        .transactionId(event.getTransactionId())
                        .accountId(event.getFromAccountId())
                        .ownerName(account.getOwnerName())
                        .message("Transaction blocked: " + event.getReason())
                        .status("REJECTED")
                        .amount(event.getAmount())
                        .timestamp(LocalDateTime.now())
                        .build());
            });

        } else {
            // Approve and forward to balance update service
            transaction.setStatus(Transaction.TransactionStatus.APPROVED);
            transactionRepository.save(transaction);
            log.info("Transaction APPROVED: transactionId={}", event.getTransactionId());

            transactionProducer.publishBalanceUpdate(BalanceUpdateEvent.builder()
                    .transactionId(transaction.getId())
                    .fromAccountId(transaction.getFromAccountId())
                    .toAccountId(transaction.getToAccountId())
                    .amount(transaction.getAmount())
                    .currency(transaction.getCurrency())
                    .timestamp(LocalDateTime.now())
                    .build());
        }
    }
}