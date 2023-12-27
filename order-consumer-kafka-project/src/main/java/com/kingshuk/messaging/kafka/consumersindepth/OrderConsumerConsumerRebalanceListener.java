package com.kingshuk.messaging.kafka.consumersindepth;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

import static com.kingshuk.messaging.kafka.consumersindepth.OrderConsumerCommon.getConsumerInDepthProperties;
import static com.kingshuk.messaging.kafka.consumersindepth.OrderConsumerCommon.processConsumerRecords;

@AllArgsConstructor
@Getter
class OrderConsumerOffsetTracking {
    private KafkaConsumer<String, Integer> consumer;
    private Map<TopicPartition, OffsetAndMetadata> offsetTracker;
}

public class OrderConsumerConsumerRebalanceListener {

    public static final String ORDER_TOPIC = "bharath-course-order-topic";

    private;

    public static void main(String[] args) {
        Properties properties = getConsumerInDepthProperties();

        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties)) {
            OrderConsumerOffsetTracking offsetTracking = new OrderConsumerOffsetTracking(consumer, new HashMap<>());
            consumer.subscribe(Collections.singletonList(ORDER_TOPIC), new RebalanceListener(offsetTracking));
            ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(40));
            for (ConsumerRecord<String, Integer> consumerRecord : consumerRecords) {
                System.out.println("Product Name: " + consumerRecord.key());
                System.out.println("Product Quantity: " + consumerRecord.value());
            }
            consumer.commitSync();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    @AllArgsConstructor

    public static class RebalanceListener implements ConsumerRebalanceListener {

        private OrderConsumerOffsetTracking offsetTracking;

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("The newly assigned partitions are" + partitions);
        }
    }


}
