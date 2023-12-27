package com.kingshuk.messaging.kafka.consumersindepth;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;

import static com.kingshuk.messaging.kafka.consumersindepth.OrderConsumerCommon.getConsumerInDepthProperties;
import static com.kingshuk.messaging.kafka.consumersindepth.OrderConsumerCommon.processConsumerRecords;

public class OrderConsumerAsyncCommit {

    public static final String ORDER_TOPIC = "bharath-course-order-topic";

    public static void main(String[] args) {
        Properties properties = getConsumerInDepthProperties();

        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties)) {
            processConsumerRecords(consumer, ORDER_TOPIC);
           consumer.commitAsync(new OffsetCommitCallback() {
               @Override
               public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                   //Our logic goes here
               }
           });
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }


}
