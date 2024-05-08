package com.kingshuk.messaging.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class StreamsAssignmentProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 15; i++) {
                ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>("kafka-streams-assignment-input-topic"
                        , "MacBook Pro", i);
                Future<RecordMetadata> send = producer.send(producerRecord);
                RecordMetadata recordMetadata = send.get();
                System.out.printf("The message went to %d partition and %d offset%n"
                        , recordMetadata.partition(), recordMetadata.offset());
                System.out.println("The message has been sent successfully");
            }

        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
