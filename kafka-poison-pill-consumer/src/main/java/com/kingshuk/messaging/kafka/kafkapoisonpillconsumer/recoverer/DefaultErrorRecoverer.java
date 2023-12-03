package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer.recoverer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class DefaultErrorRecoverer implements ConsumerRecordRecoverer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultErrorRecoverer.class);

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;
    @Override
    public void accept(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
        LOGGER.info("Inside the default handler {}", consumerRecord.value());

        MessageListenerContainer listenerContainer = endpointRegistry.getListenerContainer("kafka-poison-pill");
        if(Objects.nonNull(listenerContainer)){
            listenerContainer.stop();
        }
    }
}
