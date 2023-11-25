package com.kingshuk.messaging.kafka.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumerAvroDeserialization {

    public static final String ORDER_AVRO_TOPIC = "bharath-course-order-avro-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url","http://localhost:8081");
        properties.setProperty("specific.avro.reader","true");
        properties.setProperty("group.id", "OrderGroup");

        try(KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(ORDER_AVRO_TOPIC));

            ConsumerRecords<String, Order> consumerRecords = consumer.poll(Duration.ofMinutes(5));
            consumerRecords.forEach(consumerRecord -> {
                System.out.println("Product Name: "+ consumerRecord.key());
                Order order = consumerRecord.value();
                System.out.println("Customer Name: "+ order.getCustomerName());
                System.out.println("Product Quantity: "+ order.getQuantity());
            });
        }catch (Exception exception){
            exception.printStackTrace();
        }


    }
}
