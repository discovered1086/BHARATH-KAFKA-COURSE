package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "kafka-topic-poison-pill",
    id = "kafka-poison-pill")
    public void processOrder(ConsumerRecord<String, String> data){
        LOGGER.info("The message has been received");
        LOGGER.info("Here's the consumer name {}", data.value());
    }
}
