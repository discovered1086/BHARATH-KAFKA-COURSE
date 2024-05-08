package com.kingshuk.messaging.kafka.consumersindepth;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderConsumerConsumerRebalanceListener.class);

    public static void main(String[] args) {
        Properties properties = getConsumerInDepthProperties();
        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties)) {
            OrderConsumerOffsetTracking offsetTracking = new OrderConsumerOffsetTracking(consumer, new HashMap<>());
            consumer.subscribe(Collections.singletonList(ORDER_TOPIC), new RebalanceListener(offsetTracking));
            ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(40));

            int count = 0;

            for (ConsumerRecord<String, Integer> consumerRecord : consumerRecords) {
                //For now our processing logic is to just print the record details
                LOGGER.info("Product Name: {}", consumerRecord.key());
                LOGGER.info("Product Quantity: {}", consumerRecord.value());

                //Adding the topic and the offset information to the map at this point as
                // these records have been processed above
                TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset() + 1);
                offsetTracking.getOffsetTracker().put(topicPartition, offsetAndMetadata);

                //Now it's time to check if at least 10 records have been processed and if yes,
                //Then commit those 10 records. 10 is our batch size here.
                //<<<<<<-------If a rebalance happens at this point, meaning after the records have been
                //processed but have not been committed yet, then we use the Rebalance listener to commit these records
                // We can do that because we have saved the records to the map above---->>>>>>>>>>>>
                if (count % 10 == 0) {
                    consumer.commitAsync(Collections.singletonMap(topicPartition, offsetAndMetadata),
                            (offsets, exception) -> {
                                //Our logic after the 10 records are successfully committed.
                                LOGGER.info("Offsets committed: {}", offsets);
                                if (Objects.nonNull(exception)) {
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

    @AllArgsConstructor
    public static class RebalanceListener implements ConsumerRebalanceListener {

        private OrderConsumerOffsetTracking offsetTracking;

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOGGER.info("These partitions {} are about to be revoked", partitions);
            offsetTracking.getConsumer().commitSync(offsetTracking.getOffsetTracker());
            LOGGER.info("These partitions {} are about to be revoked", partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOGGER.info("The newly assigned partitions are: {}", partitions);
        }
    }


}
