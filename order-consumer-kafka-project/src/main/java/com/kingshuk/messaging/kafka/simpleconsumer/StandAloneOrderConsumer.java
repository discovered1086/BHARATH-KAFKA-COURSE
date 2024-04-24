package com.kingshuk.messaging.kafka.simpleconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class StandAloneOrderConsumer {

    public static final String SIMPLE_CONSUMER_TOPIC = "bharath-course-simple-consumer-topic";

    private static final Logger LOGGER = LoggerFactory.getLogger(StandAloneOrderConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "StandAloneOrderConsumer");

        //We can either build the list of partitions this way
//        List<TopicPartition> partitions = new ArrayList<>();
//        partitions.add(new TopicPartition(SIMPLE_CONSUMER_TOPIC, 0));
//        partitions.add(new TopicPartition(SIMPLE_CONSUMER_TOPIC, 1));

        //OR we can use the partitions for method
        //This method returns all the partitions for the topic. So better to use this especially when
        //We don't know the exact number of partitions in the topic
        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(SIMPLE_CONSUMER_TOPIC);

            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                    .collect(Collectors.toList());

            consumer.assign(partitions);

            ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(40));
            consumerRecords.forEach(consumerRecord -> {
                LOGGER.info("Product Name: {}" , consumerRecord.key());
                LOGGER.info("Product Quantity: {}" , consumerRecord.value());
            });

            consumer.commitAsync();
        }
    }
}
