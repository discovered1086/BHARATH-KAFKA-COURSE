package com.kingshuk.messaging.kafka.kafkaconsumerspringboot.consumer;

import com.kingshuk.messaging.kafka.kafkaproducerspringboot.model.UserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UserKafkaConsumer {
    private static final String TOPIC_NAME = "kafka-course-user-topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(UserKafkaConsumer.class);

//    @KafkaListener(topics = TOPIC_NAME)
    public void consume(int age) {
        LOGGER.info("User is {} years old", age);
    }

    @KafkaListener(topics = TOPIC_NAME)
    public void consume(UserInfo userInfo) {
        LOGGER.info("User details are {}", userInfo);
    }
}
