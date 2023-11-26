package com.kingshuk.messaging.kafka.assignments.avro;



import com.kingshuk.messaging.kafka.avro.TruckCoordinates;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class TruckingProducerAvroSerialization {

    public static final String TRUCKING_AVRO_TOPIC = "bharath-course-trucking-avro-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("value.serializer",KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url","http://localhost:8081");

        try (KafkaProducer<String, TruckCoordinates> producer = new KafkaProducer<>(properties)) {
            TruckCoordinates truckCoordinates = new TruckCoordinates(
                    UUID.randomUUID().toString(),
                    "41.8876493 N",
                    "-87.6212124 W");
            ProducerRecord<String, TruckCoordinates> producerRecord = new ProducerRecord<>(TRUCKING_AVRO_TOPIC, truckCoordinates);
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
