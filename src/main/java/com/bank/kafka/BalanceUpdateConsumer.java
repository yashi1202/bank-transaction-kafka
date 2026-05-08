package com.bank.kafka;

import com.bank.dto.TransactionEvents.*;
import com.bank.entity.Account;
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
 * Balance Update Service
 * Consumes: balance-update
 * Produces: transaction-notification
 *
 * Uses pessimistic locking to prevent race conditions.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BalanceUpdateConsumer {

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final TransactionProducer transactionProducer;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${kafka.topics.balance-update}",
            groupId = "balance-update-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeBalanceUpdate(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset) {

        BalanceUpdateEvent event = objectMapper.convertValue(payload, BalanceUpdateEvent.class);
        log.info("Processing balance update: transactionId={}", event.getTransactionId());

        Transaction transaction = transactionRepository.findById(event.getTransactionId())
                .orElseThrow(() -> new RuntimeException("Transaction not found: " + event.getTransactionId()));

        // Use pessimistic locks to prevent concurrent balance modifications
        Account fromAccount = accountRepository.findByIdWithLock(event.getFromAccountId())
                .orElseThrow(() -> new RuntimeException("Source account not found: " + event.getFromAccountId()));

        Account toAccount = accountRepository.findByIdWithLock(event.getToAccountId())
                .orElseThrow(() -> new RuntimeException("Destination account not found: " + event.getToAccountId()));

        // Check sufficient balance
        if (fromAccount.getBalance().compareTo(event.getAmount()) < 0) {
            log.warn("Insufficient balance for transactionId={}", event.getTransactionId());
            transaction.setStatus(Transaction.TransactionStatus.FAILED);
            transactionRepository.save(transaction);

            publishFailureNotification(event, fromAccount, "Insufficient balance");
            return;
        }

        // Debit source, credit destination
        fromAccount.setBalance(fromAccount.getBalance().subtract(event.getAmount()));
        toAccount.setBalance(toAccount.getBalance().add(event.getAmount()));

        accountRepository.save(fromAccount);
        accountRepository.save(toAccount);

        transaction.setStatus(Transaction.TransactionStatus.COMPLETED);
        transactionRepository.save(transaction);

        log.info("Balance updated successfully: transactionId={}, fromBalance={}, toBalance={}",
                event.getTransactionId(), fromAccount.getBalance(), toAccount.getBalance());

        // Notify sender
        transactionProducer.publishNotification(TransactionNotificationEvent.builder()
                .transactionId(event.getTransactionId())
                .accountId(fromAccount.getAccountId())
                .ownerName(fromAccount.getOwnerName())
                .phoneNumber(fromAccount.getPhoneNumber())
                .message(String.format("Transfer of %s %s to account %s completed successfully",
                        event.getAmount(), event.getCurrency(), event.getToAccountId()))
                .status("COMPLETED")
                .amount(event.getAmount())
                .timestamp(LocalDateTime.now())
                .build());

        // Notify receiver
        transactionProducer.publishNotification(TransactionNotificationEvent.builder()
                .transactionId(event.getTransactionId())
                .accountId(toAccount.getAccountId())
                .ownerName(toAccount.getOwnerName())
                .phoneNumber(toAccount.getPhoneNumber())
                .message(String.format("You received %s %s from account %s",
                        event.getAmount(), event.getCurrency(), event.getFromAccountId()))
                .status("COMPLETED")
                .amount(event.getAmount())
                .timestamp(LocalDateTime.now())
                .build());
    }
    
    

    private void publishFailureNotification(BalanceUpdateEvent event, Account account, String reason) {
        transactionProducer.publishNotification(TransactionNotificationEvent.builder()
                .transactionId(event.getTransactionId())
                .accountId(account.getAccountId())
                .ownerName(account.getOwnerName())
                .message("Transaction failed: " + reason)
                .status("FAILED")
                .amount(event.getAmount())
                .timestamp(LocalDateTime.now())
                .build());
    }
}