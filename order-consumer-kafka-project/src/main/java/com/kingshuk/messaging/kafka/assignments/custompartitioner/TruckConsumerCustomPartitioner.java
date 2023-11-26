package com.kingshuk.messaging.kafka.assignments.custompartitioner;

import com.kingshuk.messaging.kafka.assignments.customdeserialization.TruckCoordinates;
import com.kingshuk.messaging.kafka.assignments.customdeserialization.TruckDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SuppressWarnings({"java:S2189", "java:S125", "java:S106"})
public class TruckConsumerCustomPartitioner {

    public static final String TRUCKING_PARTITIONED_TOPIC = "bharath-course-trucking-partitioned-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", TruckDeserializer.class.getName());
        properties.setProperty("group.id", "trucking-consumer-partitioned-group");

        try (KafkaConsumer<String, TruckCoordinates> truckingConsumer = new KafkaConsumer<>(properties)) {
            truckingConsumer.subscribe(Collections.singletonList(TRUCKING_PARTITIONED_TOPIC));
            while (true) {
                ConsumerRecords<String, TruckCoordinates> consumerRecords = truckingConsumer.poll(Duration.ofSeconds(15));
                for (ConsumerRecord<String, TruckCoordinates> consumerRecord : consumerRecords) {
                    TruckCoordinates coordinates = consumerRecord.value();
                    System.out.println("Truck Id: " + coordinates.getTruckId());
                    System.out.println("Truck latitude: " + coordinates.getLatitude());
                    System.out.println("Truck longitude: " + coordinates.getLongitude());
                    System.out.println("Partition Used: " + consumerRecord.partition());
                }
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
