package com.kingshuk.messaging.kafka.assignments.customserialization;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class TruckingProducerCustomSerialization {

    public static final String TRUCKING_TOPIC = "bharath-course-trucking-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", TruckSerializer.class.getName());

        try (KafkaProducer<String, TruckCoordinates> producer = new KafkaProducer<>(properties)) {
            TruckCoordinates truckCoordinates = TruckCoordinates.builder()
                    .truckId(UUID.randomUUID().toString())
                    .latitude("41.8876493 N")
                    .longitude("-87.6212124 W")
                    .build();
            ProducerRecord<String, TruckCoordinates> producerRecord = new ProducerRecord<>(TRUCKING_TOPIC, truckCoordinates);
            Future<RecordMetadata> send = producer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();
            System.out.printf("The message went to %d partition and %d offset%n"
                    , recordMetadata.partition(), recordMetadata.offset());
            System.out.println("The message has been sent successfully");
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
