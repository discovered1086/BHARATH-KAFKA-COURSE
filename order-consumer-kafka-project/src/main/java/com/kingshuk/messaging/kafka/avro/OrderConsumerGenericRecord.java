package com.kingshuk.messaging.kafka.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumerGenericRecord {

    public static final String ORDER_AVRO_GENERIC_TOPIC = "bharath-course-order-avro-generic-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url","http://localhost:8081");
//        properties.setProperty("specific.avro.reader","true");
        properties.setProperty("group.id", "OrderGroup");

        try(KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(ORDER_AVRO_GENERIC_TOPIC));

            ConsumerRecords<String, GenericRecord> consumerRecords = consumer.poll(Duration.ofMinutes(5));
            consumerRecords.forEach(consumerRecord -> {
                System.out.println("Product Name: "+ consumerRecord.key());
                GenericRecord order = consumerRecord.value();
                System.out.println("Customer Name: "+ order.get("customerName"));
                System.out.println("Product Quantity: "+ order.get("quantity"));
            });
        }catch (Exception exception){
            exception.printStackTrace();
        }


    }
}
