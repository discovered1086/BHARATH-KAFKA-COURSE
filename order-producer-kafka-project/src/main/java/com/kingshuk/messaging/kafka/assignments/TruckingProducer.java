package com.kingshuk.messaging.kafka.assignments;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class TruckingProducer {

    public static final String TRUCKING_TOPIC = "bharath-course-trucking-topic";
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TRUCKING_TOPIC
                    , UUID.randomUUID().toString(), "41.8876493 N, -87.6212124 W");
            Future<RecordMetadata> send = producer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();
            System.out.printf("The message went to %d partition and %d offset%n"
                    , recordMetadata.partition(), recordMetadata.offset());
            System.out.println("The message has been sent successfully");
        }catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
