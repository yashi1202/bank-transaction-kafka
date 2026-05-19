package com.bank.service;

import com.bank.config.TwilioConfig;
import com.twilio.rest.api.v2010.account.Message;
import com.twilio.type.PhoneNumber;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class SmsService {

    private final Optional<TwilioConfig> twilioConfig;

    public void sendSms(String toPhoneNumber, String messageBody) {
        try {
            Message message = Message.creator(
                    new PhoneNumber(toPhoneNumber),   // recipient
                    new PhoneNumber(twilioConfig.get().getFromNumber()), // sender
                    messageBody
            ).create();

            log.info("✅ SMS sent successfully: sid={}, to={}, status={}",
                    message.getSid(),
                    toPhoneNumber,
                    message.getStatus());

        } catch (Exception e) {
            log.error("❌ Failed to send SMS to {}: {}", toPhoneNumber, e.getMessage());
        }
    }
}