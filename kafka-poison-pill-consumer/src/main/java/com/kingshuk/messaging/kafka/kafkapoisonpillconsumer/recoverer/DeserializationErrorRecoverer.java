package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer.recoverer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.stereotype.Component;

@Component
public class DeserializationErrorRecoverer implements ConsumerRecordRecoverer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeserializationErrorRecoverer.class);
    @Override
    public void accept(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
        LOGGER.info("Inside the deserialization error recoverer");
    }
}
