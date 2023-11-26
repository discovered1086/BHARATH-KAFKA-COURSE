package com.kingshuk.messaging.kafka.custompartitioner;

import com.kingshuk.messaging.kafka.customdeserialization.Order;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SuppressWarnings({"java:S2189", "java:S125", "java:S106"})
public class OrderConsumerCustomPartition {

    public static final String MULTIPLE_PARTITION_TOPIC = "bharath-course-multiple-partition-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "com.kingshuk.messaging.kafka.customdeserialization.OrderDeserializer");
        properties.setProperty("group.id", "OrderGroup");

        try(KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(MULTIPLE_PARTITION_TOPIC));
            while(true) {
                ConsumerRecords<String, Order> consumerRecords = consumer.poll(Duration.ofMinutes(1));
                consumerRecords.forEach(consumerRecord -> {
                    System.out.println("Product Name: " + consumerRecord.key());
                    Order order = consumerRecord.value();
                    System.out.println("Customer Name: " + order.getCustomerName());
                    System.out.println("Product Quantity: " + order.getQuantity());
                    System.out.println("Partition: " + consumerRecord.partition());
                });
            }
        }catch (Exception exception){
            exception.printStackTrace();
        }


    }
}
