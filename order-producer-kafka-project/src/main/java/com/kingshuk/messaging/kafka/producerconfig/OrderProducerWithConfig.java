package com.kingshuk.messaging.kafka.producerconfig;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

public class OrderProducerWithConfig {

    public static final String ORDER_TOPIC = "bharath-course-order-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "45895644");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "45895644");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "2");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "200");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "3096");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "500");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>(ORDER_TOPIC
                    , "MacBook Pro", 1000);
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
