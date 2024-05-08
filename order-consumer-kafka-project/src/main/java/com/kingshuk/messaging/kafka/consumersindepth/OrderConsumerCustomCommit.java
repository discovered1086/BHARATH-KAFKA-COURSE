package com.kingshuk.messaging.kafka.consumersindepth;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

import static com.kingshuk.messaging.kafka.consumersindepth.OrderConsumerCommon.getConsumerInDepthProperties;

public class OrderConsumerCustomCommit {

    public static final String ORDER_TOPIC = "bharath-course-order-topic";

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderConsumerCustomCommit.class);

    public static void main(String[] args) {
        Properties properties = getConsumerInDepthProperties();

        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(ORDER_TOPIC));
            ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(40));
            int count = 0;
            for (ConsumerRecord<String, Integer> consumerRecord : consumerRecords) {
                LOGGER.info("Product Name: {}", consumerRecord.key());
                LOGGER.info("Product Quantity: {}" , consumerRecord.value());

                if (count % 10 == 0) {
                    consumer.commitAsync(Collections.singletonMap(
                                    new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                                    new OffsetAndMetadata(consumerRecord.offset() + 1)),
                            (offsets, exception) -> {
                                //Our logic when the 10 records are successfully committed.
                                LOGGER.info("Offsets committed: {}", offsets);
                                if(Objects.nonNull(exception)){
                                    LOGGER.error("Commit failed", exception);
                                }
                            });
                }

                count++;
            }

        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }


}
