package com.bank;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.bank.config.TestTwilioConfig;

@SpringBootTest
@Import(TestTwilioConfig.class)
class BankTransactionKafkaApplicationTests {

	@Test
	void contextLoads() {
	}

}
