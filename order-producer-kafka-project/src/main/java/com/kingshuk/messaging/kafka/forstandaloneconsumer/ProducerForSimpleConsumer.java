package com.kingshuk.messaging.kafka.forstandaloneconsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerForSimpleConsumer {

    public static final String SIMPLE_CONSUMER_TOPIC = "bharath-course-simple-consumer-topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerForSimpleConsumer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>(SIMPLE_CONSUMER_TOPIC
                    , "MacBook Pro", 1500);
            Future<RecordMetadata> send = producer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();
            LOGGER.info("The message went to {} partition and {} offset"
                    , recordMetadata.partition(), recordMetadata.offset());
            LOGGER.info("The message has been sent successfully");
        }
    }
}
