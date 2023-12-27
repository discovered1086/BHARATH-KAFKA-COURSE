package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "kafka-topic-poison-pill",
            id = "kafka-poison-pill", idIsGroup = false, containerFactory = "listenerContainerFactory")
    public void processOrder(ConsumerRecord<String, GenericRecord> data, Acknowledgment acknowledgment) throws KafkaPoisonPillException {
        LOGGER.info("The message has been received");
        LOGGER.info("Here's the consumer name {}", data.value());
        throw new KafkaPoisonPillException("Throwing an error from the listener");
//        acknowledgment.acknowledge();
    }
}
