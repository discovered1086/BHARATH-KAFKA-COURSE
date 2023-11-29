package com.kingshuk.messaging.kafka.transactions;

import org.apache.kafka.clients.producer.*;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OrderProducerWithTransaction {

    public static final String ORDER_TRANSACTION_TOPIC = "bharath-course-order-transaction-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
//        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties);
        try {
            producer.initTransactions();
            List<ProducerRecord<String, Integer>> records = List.of(new ProducerRecord<>(ORDER_TRANSACTION_TOPIC
                            , "MacBook Pro", 1000),
                    new ProducerRecord<>(ORDER_TRANSACTION_TOPIC
                            , "Iphone", 899));

            //Transaction starts
            producer.beginTransaction();
            records.forEach(theRecord -> {
                Future<RecordMetadata> send = producer.send(theRecord);
                try {
                    RecordMetadata recordMetadata = send.get();
                    System.out.printf("The message went to %d partition and %d offset%n"
                            , recordMetadata.partition(), recordMetadata.offset());
                    System.out.println("The message has been sent successfully");
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            producer.commitTransaction();

        } catch (Exception exception) {
            producer.abortTransaction();
            exception.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
