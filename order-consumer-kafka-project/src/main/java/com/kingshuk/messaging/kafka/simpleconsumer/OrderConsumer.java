package com.kingshuk.messaging.kafka.simpleconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static final String ORDER_TOPIC = "bharath-course-order-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.setProperty("group.id", "OrderGroup");
        properties.setProperty("enable.auto.commit", "false");

        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(ORDER_TOPIC));

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
