package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

//@Component
public class SimpleProducer implements CommandLineRunner {

//    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);
//
//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void run(String... args) throws Exception {
//        kafkaTemplate.send("kafka-topic-poison-pill", "record2", "You have been warned");
//        LOGGER.info("Message sent");
    }
}
