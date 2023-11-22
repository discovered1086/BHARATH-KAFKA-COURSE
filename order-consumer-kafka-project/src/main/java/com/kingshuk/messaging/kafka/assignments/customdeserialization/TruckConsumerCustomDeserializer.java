package com.kingshuk.messaging.kafka.assignments.customdeserialization;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TruckConsumerCustomDeserializer {

    public static final String TRUCKING_TOPIC = "bharath-course-trucking-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", TruckDeserializer.class.getName());
        properties.setProperty("group.id", "trucking-consumer-group");

        try (KafkaConsumer<String, TruckCoordinates> truckingConsumer = new KafkaConsumer<>(properties)) {
            truckingConsumer.subscribe(Collections.singletonList(TRUCKING_TOPIC));

            ConsumerRecords<String, TruckCoordinates> consumerRecords = truckingConsumer.poll(Duration.ofMinutes(1));
            for (ConsumerRecord<String, TruckCoordinates> consumerRecord : consumerRecords) {
                TruckCoordinates coordinates = consumerRecord.value();
                System.out.println("Truck Id: " + coordinates.getTruckId());
                System.out.println("Truck latitude: " + coordinates.getLatitude());
                System.out.println("Truck longitude: " + coordinates.getLongitude());
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
