package com.kingshuk.messaging.kafka.custompartitioner;

import com.kingshuk.messaging.kafka.customserialization.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class OrderProducerWithCustomPartitioner {

    public static final String MULTIPLE_PARTITION_TOPIC = "bharath-course-multiple-partition-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "com.kingshuk.messaging.kafka.customserialization.OrderSerializer");
        properties.setProperty("partitioner.class", OrderPartitioner.class.getName());

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(properties)) {
            Order order = Order.builder()
                    .productName("MacBook Pro 2023")
                    .customerName("Kingshuk Mukherjee")
                    .quantity(135)
                    .build();
            ProducerRecord<String, Order> producerRecord = new ProducerRecord<>(MULTIPLE_PARTITION_TOPIC
                    , "MacBook Pro 2023", order);
            Future<RecordMetadata> send = producer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();
            System.out.println("The message has been sent successfully");
            System.out.printf("Message key: %s %n partition: %d%n offset: %d"
                    , order.getProductName(), recordMetadata.partition(), recordMetadata.offset());
        }catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}
