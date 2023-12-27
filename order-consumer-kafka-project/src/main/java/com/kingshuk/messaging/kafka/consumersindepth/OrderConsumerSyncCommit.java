package com.kingshuk.messaging.kafka.consumersindepth;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

import static com.kingshuk.messaging.kafka.consumersindepth.OrderConsumerCommon.getConsumerInDepthProperties;
import static com.kingshuk.messaging.kafka.consumersindepth.OrderConsumerCommon.processConsumerRecords;

public class OrderConsumerSyncCommit {

    public static final String ORDER_TOPIC = "bharath-course-order-topic";

    public static void main(String[] args) {
        Properties properties = getConsumerInDepthProperties();

        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties)) {
            processConsumerRecords(consumer, ORDER_TOPIC);
            consumer.commitSync();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }


}
