package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProducer implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderProducer.class);

    @Autowired
    private KafkaTemplate<String, GenericRecord> kafkaTemplate;

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("Inside the order producer");
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("""
                {
                  "namespace": "com.kingshuk.messaging.kafka.avro",
                  "type": "record",
                  "name": "Order",
                  "fields": [
                    {
                      "name": "customerName",
                      "type": "string"
                    },
                    {
                      "name": "productName",
                      "type": "string"
                    },
                    {
                      "name": "quantity",
                      "type": "int"
                    }
                  ]
                }
                """);
        GenericData.Record order = new GenericData.Record(schema);
        order.put("customerName", "Kingshuk Mukherjee");
        order.put("productName", "The Happiness Trap");
        order.put("quantity", 900);
        kafkaTemplate.send("kafka-topic-poison-pill", "record1", order);
        LOGGER.info("Message sent");
    }
}
