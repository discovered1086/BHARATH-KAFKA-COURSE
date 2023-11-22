package com.kingshuk.messaging.kafka.customserialization;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class OrderProducerWithCustomSerialization {

    public static final String ORDER_TOPIC = "bharath-course-order-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "com.kingshuk.messaging.kafka.customserialization.OrderSerializer");

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(properties)) {
            Order order = Order.builder()
                    .productName("MacBook Pro")
                    .customerName("Kingshuk Mukherjee")
                    .quantity(135)
                    .build();
            ProducerRecord<String, Order> producerRecord = new ProducerRecord<>(ORDER_TOPIC
                    , "MacBook Pro", order);
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
