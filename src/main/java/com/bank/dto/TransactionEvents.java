package com.bank.dto;



import lombok.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public class TransactionEvents {

    /** Published when a new transaction is created */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TransactionInitiatedEvent {
        private UUID transactionId;
        private String fromAccountId;
        private String toAccountId;
        private BigDecimal amount;
        private String currency;
        private String type;
        private String description;
        private LocalDateTime timestamp;
    }

    /** Published by Fraud Detection after checking */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class FraudCheckResultEvent {
        private UUID transactionId;
        private String fromAccountId;
        private BigDecimal amount;
        private boolean fraudulent;
        private String reason;
        private double riskScore;
        private LocalDateTime timestamp;
    }

    /** Published after a transaction is approved, triggers balance update */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class BalanceUpdateEvent {
        private UUID transactionId;
        private String fromAccountId;
        private String toAccountId;
        private BigDecimal amount;
        private String currency;
        private LocalDateTime timestamp;
    }

    /** Published after any significant status change */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TransactionNotificationEvent {
        private UUID transactionId;
        private String accountId;
        private String ownerName;
        private String phoneNumber;
        private String message;
        private String status;
        private BigDecimal amount;
        private LocalDateTime timestamp;
    }
}