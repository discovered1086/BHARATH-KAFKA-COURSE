package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class ContainerScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerScheduler.class);

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Scheduled(fixedRate = 60 * 1000)
    public void containerStart() {
        LOGGER.info("Inside the container scheduler....");
        MessageListenerContainer listenerContainer = endpointRegistry
                .getListenerContainer("kafka-poison-pill");
        if (!Objects.requireNonNull(listenerContainer).isRunning()) {
            listenerContainer.start();
        }
    }
}
