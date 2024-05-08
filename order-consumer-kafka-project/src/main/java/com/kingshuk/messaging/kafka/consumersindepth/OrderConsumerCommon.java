package com.kingshuk.messaging.kafka.consumersindepth;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumerCommon {

    private OrderConsumerCommon(){
        throw new UnsupportedOperationException("This is not allowed");
    }

//    protected static Properties getConsumerInDepthProperties() {
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
//        properties.setProperty("group.id", "OrderGroup");
//        properties.setProperty("auto.commit.offset", "false");
//        return properties;
//    }

    protected static Properties getConsumerInDepthProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //Data transfer related properties
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "102455451");
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");

        //Heartbeat and rebalance related properties
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");

        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1MB");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "OrderConsumer");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

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
