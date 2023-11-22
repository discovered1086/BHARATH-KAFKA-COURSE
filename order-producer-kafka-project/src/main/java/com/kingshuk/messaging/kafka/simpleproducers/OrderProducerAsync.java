package com.kingshuk.messaging.kafka.simpleproducers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducerAsync {

    public static final String ORDER_TOPIC = "bharath-course-order-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>(ORDER_TOPIC
                    , "MacBook Pro", 1000);
            producer.send(producerRecord, new OrderCallback());
            System.out.println("The message has been sent successfully");
        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}
