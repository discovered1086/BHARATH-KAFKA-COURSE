package com.kingshuk.messaging.kafka.kafkaproducerspringboot.assignment;

import com.kingshuk.messaging.kafka.kafkaproducerspringboot.model.UserInfo;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class CreditCardKafkaProducer {

    private static final String TOPIC_NAME = "test_topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(CreditCardKafkaProducer.class);

    private final KafkaTemplate<String, CreditCard> kafkaTemplate;

    public void sendMessage(CreditCard creditCard) {
        LOGGER.info("Sending message with key {}: and value {}",
                creditCard.getCardNumber(), creditCard);
        kafkaTemplate.send(TOPIC_NAME, creditCard);
    }
}
