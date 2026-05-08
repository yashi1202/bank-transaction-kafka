package com.bank.service;

import com.bank.dto.TransactionDTOs.*;
import com.bank.dto.TransactionEvents.*;
import com.bank.kafka.TransactionProducer;
import com.bank.entity.Account;
import com.bank.entity.Transaction;
import com.bank.repository.AccountRepository;
import com.bank.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountRepository accountRepository;
    private final TransactionProducer transactionProducer;

    @Transactional
    public TransactionResponse initiateTransaction(TransactionRequest request) {
        // Validate accounts exist
        accountRepository.findById(request.getFromAccountId())
                .orElseThrow(() -> new RuntimeException("Source account not found: " + request.getFromAccountId()));
        accountRepository.findById(request.getToAccountId())
                .orElseThrow(() -> new RuntimeException("Destination account not found: " + request.getToAccountId()));

        // Persist transaction with INITIATED status
        Transaction transaction = Transaction.builder()
                .fromAccountId(request.getFromAccountId())
                .toAccountId(request.getToAccountId())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .status(Transaction.TransactionStatus.INITIATED)
                .type(Transaction.TransactionType.valueOf(
                        request.getType() != null ? request.getType() : "TRANSFER"))
                .description(request.getDescription())
                .build();

        transaction = transactionRepository.save(transaction);
        log.info("Transaction saved: id={}", transaction.getId());

        // Publish event to Kafka — starts the event-driven chain
        transactionProducer.publishTransactionInitiated(TransactionInitiatedEvent.builder()
                .transactionId(transaction.getId())
                .fromAccountId(transaction.getFromAccountId())
                .toAccountId(transaction.getToAccountId())
                .amount(transaction.getAmount())
                .currency(transaction.getCurrency())
                .type(transaction.getType().name())
                .description(transaction.getDescription())
                .timestamp(LocalDateTime.now())
                .build());

        return TransactionResponse.builder()
                .transactionId(transaction.getId())
                .status("INITIATED")
                .message("Transaction submitted for processing. It will be verified and processed shortly.")
                .timestamp(LocalDateTime.now())
                .build();
    }

    public Transaction getTransaction(UUID id) {
        return transactionRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Transaction not found: " + id));
    }

    public List<Transaction> getAccountTransactions(String accountId) {
        return transactionRepository
                .findByFromAccountIdOrToAccountIdOrderByCreatedAtDesc(accountId, accountId);
    }
}