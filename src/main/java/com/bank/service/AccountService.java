package com.bank.service;

import com.bank.dto.TransactionDTOs.AccountRequest;
import com.bank.entity.Account;
import com.bank.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;

    public Account createAccount(AccountRequest request) {
        if (accountRepository.existsById(request.getAccountId())) {
            throw new RuntimeException("Account already exists: " + request.getAccountId());
        }

        Account account = Account.builder()
                .accountId(request.getAccountId())
                .ownerName(request.getOwnerName())
                .balance(request.getInitialBalance())
                .currency(request.getCurrency())
                .status(Account.AccountStatus.ACTIVE)
                .build();

        account = accountRepository.save(account);
        log.info("Account created: accountId={}, owner={}", account.getAccountId(), account.getOwnerName());
        return account;
    }

    public Account getAccount(String accountId) {
        return accountRepository.findById(accountId)
                .orElseThrow(() -> new RuntimeException("Account not found: " + accountId));
    }

    public List<Account> getAllAccounts() {
        return accountRepository.findAll();
    }
}