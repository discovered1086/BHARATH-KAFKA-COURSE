package com.kingshuk.messaging.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class StreamsAssignmentConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.setProperty("group.id", "StreamsConsumerGroup");
        properties.setProperty("enable.auto.commit", "false");

        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("kafka-streams-assignment-output-topic"));

            ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(40));
            consumerRecords.forEach(consumerRecord -> {
                System.out.println("Product Name: " + consumerRecord.key());
                System.out.println("Product Quantity: " + consumerRecord.value());
            });
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
