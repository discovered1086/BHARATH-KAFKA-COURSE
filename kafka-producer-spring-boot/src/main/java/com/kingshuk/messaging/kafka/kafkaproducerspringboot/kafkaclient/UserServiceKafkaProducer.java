package com.kingshuk.messaging.kafka.kafkaproducerspringboot.kafkaclient;

import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class UserServiceKafkaProducer {
    private static final String TOPIC_NAME = "kafka-course-user-topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(UserServiceKafkaProducer.class);

    private final KafkaTemplate<String, Integer> kafkaTemplate;

    public void sendMessage(String name, int age) {
        LOGGER.info("Sending message with key: {} and value: {}", name, age);
        kafkaTemplate.send(TOPIC_NAME, name, age);
    }
}
