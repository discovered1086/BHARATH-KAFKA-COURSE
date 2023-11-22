package com.kingshuk.messaging.kafka.customdeserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class OrderDeserializer implements Deserializer<Order> {
    @Override
    public Order deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(data, Order.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
