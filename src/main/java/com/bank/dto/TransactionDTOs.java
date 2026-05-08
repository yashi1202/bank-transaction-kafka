package com.bank.dto;

import jakarta.validation.constraints.*;
import lombok.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public class TransactionDTOs {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TransactionRequest {
        @NotBlank(message = "Source account is required")
        private String fromAccountId;

        @NotBlank(message = "Destination account is required")
        private String toAccountId;

        @NotNull(message = "Amount is required")
        @DecimalMin(value = "0.01", message = "Amount must be greater than 0")
        private BigDecimal amount;

        @NotBlank(message = "Currency is required")
        private String currency;

        private String type;
        private String description;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TransactionResponse {
        private UUID transactionId;
        private String status;
        private String message;
        private LocalDateTime timestamp;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class AccountRequest {
        @NotBlank
        private String accountId;

        @NotBlank
        private String ownerName;

        @NotNull
        @DecimalMin("0.00")
        private BigDecimal initialBalance;

        @NotBlank
        private String currency;
    }
}