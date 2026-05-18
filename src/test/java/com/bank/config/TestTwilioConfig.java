package com.bank.config;

import com.twilio.Twilio;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class TestTwilioConfig {

    @Bean
    @Primary
    public TwilioConfig twilioConfig() {
        return new MockTwilioConfig();
    }

    @Getter
    static class MockTwilioConfig extends TwilioConfig {
        private String accountSid = "test-account-sid";
        private String authToken = "test-auth-token";
        private String fromNumber = "+1234567890";

        @PostConstruct
        public void initTwilio() {
            // Skip Twilio initialization in tests
        }
    }
}