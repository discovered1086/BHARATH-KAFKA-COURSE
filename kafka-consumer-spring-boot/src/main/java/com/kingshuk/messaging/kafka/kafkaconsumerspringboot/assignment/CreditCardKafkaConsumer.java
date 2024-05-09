package com.kingshuk.messaging.kafka.kafkaconsumerspringboot.assignment;

import com.kingshuk.messaging.kafka.kafkaproducerspringboot.assignment.CreditCard;
import com.kingshuk.messaging.kafka.kafkaproducerspringboot.model.UserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CreditCardKafkaConsumer {
    private static final String TOPIC_NAME = "kafka-course-assignment-credit-card-topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(CreditCardKafkaConsumer.class);

    @KafkaListener(topics = TOPIC_NAME)
    public void consume(CreditCard creditCard) {
        LOGGER.info("The credit card details are {}", creditCard);
    }
}
