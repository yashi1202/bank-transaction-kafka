package com.bank.kafka;

import com.bank.dto.TransactionEvents.TransactionNotificationEvent;
import com.bank.service.SmsService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Notification Service
 * Consumes: transaction-notification
 *
 * In production, this would integrate with:
 * - Email (SendGrid / AWS SES)
 * - SMS (Twilio)
 * - Push notifications (Firebase)
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationConsumer {

    private final ObjectMapper objectMapper;
    private final SmsService smsService;

    @KafkaListener(
            topics = "${kafka.topics.notification}",
            groupId = "notification-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeNotification(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset) {

        TransactionNotificationEvent event = objectMapper.convertValue(payload, TransactionNotificationEvent.class);

        log.info("📧 NOTIFICATION | accountId={} | owner={} | status={} | amount={} | message={}",
                event.getAccountId(),
                event.getOwnerName(),
                event.getStatus(),
                event.getAmount(),
                event.getMessage());

        // TODO: Integrate with email/SMS provider
        sendSmsNotification(event);
    }
    
    private void sendSmsNotification(TransactionNotificationEvent event) {
        // Skip if no phone number on the account
        if (event.getPhoneNumber() == null || event.getPhoneNumber().isBlank()) {
            log.warn("⚠️ No phone number for accountId={}, skipping SMS", event.getAccountId());
            return;
        }
        if (((CharSequence) smsService).isEmpty()) {
            log.warn("⚠️ SMS service not enabled, skipping SMS notification");
            return;
        }

        String smsMessage = buildSmsMessage(event);
        smsService.sendSms(event.getPhoneNumber(), smsMessage);
    }
    
    private String buildSmsMessage(TransactionNotificationEvent event) {
        return switch (event.getStatus()) {
            case "COMPLETED" -> String.format(
                    "✅ BANK ALERT: %s\nTransaction ID: %s\nAmount: %s %s\n%s",
                    event.getOwnerName(),
                    event.getTransactionId(),
                    event.getAmount(),
                    "USD",
                    event.getMessage()
            );
            case "REJECTED" -> String.format(
                    "🚫 BANK ALERT: %s\nTransaction BLOCKED\nAmount: %s USD\nReason: %s",
                    event.getOwnerName(),
                    event.getAmount(),
                    event.getMessage()
            );
            case "FAILED" -> String.format(
                    "❌ BANK ALERT: %s\nTransaction FAILED\nAmount: %s USD\nReason: %s",
                    event.getOwnerName(),
                    event.getAmount(),
                    event.getMessage()
            );
            default -> String.format(
                    "ℹ️ BANK ALERT: %s\nTransaction Update: %s",
                    event.getOwnerName(),
                    event.getMessage()
            );
        };
    }

    private void sendNotification(TransactionNotificationEvent event) {
        // Simulate sending email/SMS
        switch (event.getStatus()) {
            case "COMPLETED" -> log.info("✅ [EMAIL] Sent success notification to account: {}", event.getAccountId());
            case "REJECTED"  -> log.warn("🚫 [EMAIL] Sent rejection notification to account: {}", event.getAccountId());
            case "FAILED"    -> log.error("❌ [EMAIL] Sent failure notification to account: {}", event.getAccountId());
            default          -> log.info("ℹ️ [EMAIL] Sent status update to account: {}", event.getAccountId());
        }
    }

    /** Dead Letter Topic handler - logs unprocessable messages */
    @KafkaListener(
            topics = "${kafka.topics.dead-letter}",
            groupId = "dlt-handler-group"
    )
    public void consumeDeadLetter(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("💀 DEAD LETTER MESSAGE received on topic={}: payload={}", topic, payload);
        // Alert ops team, store for manual review, etc.
    }
}