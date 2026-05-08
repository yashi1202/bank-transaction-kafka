package com.bank.controller;

import com.bank.dto.TransactionDTOs.*;
import com.bank.entity.Transaction;
import com.bank.service.TransactionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {

    private final TransactionService transactionService;

    /**
     * POST /api/transactions
     * Initiates a new bank transaction and publishes it to Kafka
     */
    @PostMapping
    public ResponseEntity<TransactionResponse> initiateTransaction(
            @Valid @RequestBody TransactionRequest request) {
        log.info("Transaction request received: from={}, to={}, amount={}",
                request.getFromAccountId(), request.getToAccountId(), request.getAmount());
        TransactionResponse response = transactionService.initiateTransaction(request);
        return ResponseEntity.accepted().body(response);
    }

    /**
     * GET /api/transactions/{id}
     * Get transaction status by ID
     */
    @GetMapping("/{id}")
    public ResponseEntity<Transaction> getTransaction(@PathVariable("id") UUID id) {
        return ResponseEntity.ok(transactionService.getTransaction(id));
    }

    /**
     * GET /api/transactions/account/{accountId}
     * Get all transactions for an account
     */
    @GetMapping("/account/{accountId}")
    public ResponseEntity<List<Transaction>> getAccountTransactions(@PathVariable("accountId") String accountId) {
        return ResponseEntity.ok(transactionService.getAccountTransactions(accountId));
    }
}