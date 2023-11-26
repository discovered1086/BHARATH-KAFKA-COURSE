package com.kingshuk.messaging.kafka.assignments.custompartitioner;

import com.kingshuk.messaging.kafka.assignments.customserialization.TruckCoordinates;
import com.kingshuk.messaging.kafka.assignments.customserialization.TruckSerializer;
import com.kingshuk.messaging.kafka.custompartitioner.OrderPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class TruckingProducerCustomPartitioner {

    public static final String TRUCKING_PARTITIONED_TOPIC = "bharath-course-trucking-partitioned-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", TruckSerializer.class.getName());
        properties.setProperty("partitioner.class", TruckCoordinatesPartitioner.class.getName());

        try (KafkaProducer<String, TruckCoordinates> producer = new KafkaProducer<>(properties)) {
            TruckCoordinates truckCoordinates = TruckCoordinates.builder()
                    .truckId(UUID.randomUUID().toString())
                    .latitude("37.2431")
                    .longitude("115.793")
                    .build();
            ProducerRecord<String, TruckCoordinates> producerRecord = new ProducerRecord<>(TRUCKING_PARTITIONED_TOPIC,
                    truckCoordinates.getTruckId(),
                    truckCoordinates);
            Future<RecordMetadata> send = producer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();
            System.out.printf("The message went to %d partition and %d offset%n"
                    , recordMetadata.partition(), recordMetadata.offset());
            System.out.println("The message has been sent successfully");
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
