package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DltConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DltConsumer.class);

    @KafkaListener(topics = "kafka-topic-poison-pill.DLT",
    id = "kafka-poison-pill-dlt", containerFactory = "bytesArrayListenerContainerFactory")
    public void processOrder(ConsumerRecord<String, byte[]> data){
        LOGGER.info("The poison pill value {}", new String(data.value()));
    }
}
