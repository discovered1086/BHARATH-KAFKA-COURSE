package com.kingshuk.messaging.kafka.consumersindepth;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumerCommon {

    protected static Properties getConsumerInDepthProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.setProperty("group.id", "OrderGroup");
        properties.setProperty("auto.commit.offset", "false");
        return properties;
    }

    protected static void processConsumerRecords(KafkaConsumer<String, Integer> consumer, String topic) {
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(40));
        consumerRecords.forEach(consumerRecord -> {
            System.out.println("Product Name: " + consumerRecord.key());
            System.out.println("Product Quantity: " + consumerRecord.value());
        });
    }
}
