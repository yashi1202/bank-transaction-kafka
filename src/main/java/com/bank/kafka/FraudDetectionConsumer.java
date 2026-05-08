package com.bank.kafka;

import com.bank.dto.TransactionEvents.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Random;

/**
 * Fraud Detection Service
 * Consumes: transaction-initiated
 * Produces: fraud-check-result
 *
 * Rules:
 * - Amount > 10,000 → high risk
 * - Amount > 50,000 → auto-reject
 * - Random 5% chance of flagging (simulates ML model)
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionConsumer {

    private final TransactionProducer transactionProducer;
    private final ObjectMapper objectMapper;
    private final Random random = new Random();

    @KafkaListener(
            topics = "${kafka.topics.transaction-initiated}",
            groupId = "fraud-detection-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeTransactionInitiated(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Fraud check received: topic={}, partition={}, offset={}", topic, partition, offset);

        TransactionInitiatedEvent event = objectMapper.convertValue(payload, TransactionInitiatedEvent.class);
        log.info("Running fraud check for transactionId={}, amount={}", event.getTransactionId(), event.getAmount());

        FraudCheckResultEvent result = performFraudCheck(event);
        transactionProducer.publishFraudCheckResult(result);
    }

    private FraudCheckResultEvent performFraudCheck(TransactionInitiatedEvent event) {
        BigDecimal amount = event.getAmount();
        boolean fraudulent = false;
        String reason = "Transaction looks normal";
        double riskScore = 0.1;

        // Rule 1: Very high amount
        if (amount.compareTo(new BigDecimal("50000")) > 0) {
            fraudulent = true;
            reason = "Transaction amount exceeds maximum threshold ($50,000)";
            riskScore = 0.95;
        }
        // Rule 2: High amount - elevated risk but not auto-reject
        else if (amount.compareTo(new BigDecimal("10000")) > 0) {
            riskScore = 0.65;
            reason = "High-value transaction - requires review";
            // 30% chance flagged as fraud for high-value
            if (random.nextDouble() < 0.30) {
                fraudulent = true;
                reason = "Suspicious high-value transaction pattern";
            }
        }
        // Rule 3: Random 5% flag (simulates ML anomaly detection)
        else if (random.nextDouble() < 0.05) {
            fraudulent = true;
            riskScore = 0.75;
            reason = "Unusual transaction pattern detected";
        }

        log.info("Fraud check result: transactionId={}, fraudulent={}, riskScore={}, reason={}",
                event.getTransactionId(), fraudulent, riskScore, reason);

        return FraudCheckResultEvent.builder()
                .transactionId(event.getTransactionId())
                .fromAccountId(event.getFromAccountId())
                .amount(event.getAmount())
                .fraudulent(fraudulent)
                .reason(reason)
                .riskScore(riskScore)
                .timestamp(LocalDateTime.now())
                .build();
    }
}