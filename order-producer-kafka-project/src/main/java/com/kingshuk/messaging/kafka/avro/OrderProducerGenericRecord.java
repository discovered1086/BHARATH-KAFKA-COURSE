package com.kingshuk.messaging.kafka.avro;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class OrderProducerGenericRecord {

    public static final String ORDER_AVRO_GENERIC_TOPIC = "bharath-course-order-avro-generic-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url","http://localhost:8081");

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties)) {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse("{\n" +
                    "  \"namespace\": \"com.kingshuk.messaging.kafka.avro\",\n" +
                    "  \"type\": \"record\",\n" +
                    "  \"name\": \"Order\",\n" +
                    "  \"fields\": [\n" +
                    "    {\n" +
                    "      \"name\": \"customerName\",\n" +
                    "      \"type\": \"string\"\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"name\": \"productName\",\n" +
                    "      \"type\": \"string\"\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"name\": \"quantity\",\n" +
                    "      \"type\": \"int\"\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}");
            GenericData.Record order = new GenericData.Record(schema);
            order.put("customerName", "Kingshuk Mukherjee");
            order.put("productName", "The Happiness Trap");
            order.put("quantity", 900);

            ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(ORDER_AVRO_GENERIC_TOPIC
                    , order.get("productName").toString(), order);
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
