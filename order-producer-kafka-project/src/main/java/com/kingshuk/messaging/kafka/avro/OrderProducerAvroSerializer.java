package com.kingshuk.messaging.kafka.avro;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class OrderProducerAvroSerializer {

    public static final String ORDER_AVRO_TOPIC = "bharath-course-order-avro-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url","http://localhost:8081");

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(properties)) {
            Order order = new Order();
            order.setCustomerName("Kingshuk Mukherjee");
            order.setQuantity(900);
            order.setProductName("Happiness");
            ProducerRecord<String, Order> producerRecord = new ProducerRecord<>(ORDER_AVRO_TOPIC
                    , order.getProductName(), order);
            Future<RecordMetadata> send = producer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();
            System.out.printf("The message went to %d partition and %d offset%n"
                    , recordMetadata.partition(), recordMetadata.offset());
            System.out.println("The message has been sent successfully");
        }catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}
