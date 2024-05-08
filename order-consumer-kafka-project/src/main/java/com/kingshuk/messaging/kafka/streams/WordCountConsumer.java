package com.kingshuk.messaging.kafka.streams;

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

public class WordCountConsumer {

    public static final String KAFKA_STREAMS_WORD_COUNT_TOPIC = "kafka-streams-word-count-output-topic";

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "StandAloneWordCountConsumer");

        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(KAFKA_STREAMS_WORD_COUNT_TOPIC);

            while (true) {
                List<TopicPartition> partitions = partitionInfos.stream()
                        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                        .collect(Collectors.toList());

                consumer.assign(partitions);

                ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(40));
                consumerRecords.forEach(consumerRecord -> {
                    LOGGER.info("Word: {}  | Count {}", consumerRecord.key(), consumerRecord.value());
                });

                consumer.commitAsync();
            }
        }
    }
}
