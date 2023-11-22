package com.kingshuk.messaging.kafka.assignments;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TruckConsumer {

    public static final String TRUCKING_TOPIC = "bharath-course-trucking-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "trucking-consumer-group");

        try (KafkaConsumer<String, String> truckingConsumer = new KafkaConsumer<>(properties)) {
            truckingConsumer.subscribe(Collections.singletonList(TRUCKING_TOPIC));

            ConsumerRecords<String, String> consumerRecords = truckingConsumer.poll(Duration.ofMinutes(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("Truck Id: " + consumerRecord.key());
                String[] split = consumerRecord.value().split(",");
                System.out.println("Truck latitude: " + split[0]);
                System.out.println("Truck longitude: " + split[1]);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
