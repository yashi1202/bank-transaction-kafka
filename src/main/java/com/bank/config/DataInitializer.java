package com.bank.config;

import com.bank.entity.Account;
import com.bank.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
@Slf4j
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final AccountRepository accountRepository;

    @Override
    public void run(String... args) {
        if (accountRepository.count() == 0) {
            accountRepository.save(Account.builder()
                    .accountId("ACC-001")
                    .ownerName("Alice Johnson")
                    .balance(new BigDecimal("50000.00"))
                    .currency("USD")
                    .status(Account.AccountStatus.ACTIVE)
                    .build());

            accountRepository.save(Account.builder()
                    .accountId("ACC-002")
                    .ownerName("Bob Smith")
                    .balance(new BigDecimal("25000.00"))
                    .currency("USD")
                    .status(Account.AccountStatus.ACTIVE)
                    .build());

            accountRepository.save(Account.builder()
                    .accountId("ACC-003")
                    .ownerName("Carol White")
                    .balance(new BigDecimal("100000.00"))
                    .currency("USD")
                    .status(Account.AccountStatus.ACTIVE)
                    .build());

            log.info("✅ Sample accounts seeded: ACC-001 (Alice), ACC-002 (Bob), ACC-003 (Carol)");
        }
    }
}