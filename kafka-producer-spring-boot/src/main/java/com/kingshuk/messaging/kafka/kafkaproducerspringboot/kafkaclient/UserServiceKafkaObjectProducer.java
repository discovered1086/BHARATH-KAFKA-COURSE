package com.kingshuk.messaging.kafka.kafkaproducerspringboot.kafkaclient;

import com.kingshuk.messaging.kafka.kafkaproducerspringboot.model.UserInfo;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class UserServiceKafkaObjectProducer {

    private static final String TOPIC_NAME = "kafka-course-user-topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(UserServiceKafkaObjectProducer.class);

    private final KafkaTemplate<String, UserInfo> kafkaTemplate;

    public void sendMessage(UserInfo userInfo) {
        LOGGER.info("Sending message with key {}: and value {}", userInfo.getName(), userInfo);
        kafkaTemplate.send(TOPIC_NAME, userInfo);
    }
}
