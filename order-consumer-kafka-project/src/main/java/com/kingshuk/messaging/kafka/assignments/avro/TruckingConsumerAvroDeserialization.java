package com.kingshuk.messaging.kafka.assignments.avro;

import com.kingshuk.messaging.kafka.avro.Order;
import com.kingshuk.messaging.kafka.avro.TruckCoordinates;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SuppressWarnings("java:S2189")
public class TruckingConsumerAvroDeserialization {

    public static final String TRUCKING_AVRO_TOPIC = "bharath-course-trucking-avro-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        properties.setProperty("specific.avro.reader", "true");
        properties.setProperty("group.id", "trucking-consumer-group");

        try (KafkaConsumer<String, TruckCoordinates> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TRUCKING_AVRO_TOPIC));
            while (true) {
                ConsumerRecords<String, TruckCoordinates> consumerRecords = consumer.poll(Duration.ofMinutes(5));
                consumerRecords.forEach(consumerRecord -> {
                    TruckCoordinates coordinates = consumerRecord.value();
                    System.out.println("Truck Id: " + coordinates.getTruckId());
                    System.out.println("Truck latitude: " + coordinates.getLatitude());
                    System.out.println("Truck longitude: " + coordinates.getLongitude());
                });
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }


    }
}
