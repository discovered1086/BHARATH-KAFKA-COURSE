package com.kingshuk.messaging.kafka.assignments.customdeserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TruckDeserializer implements Deserializer<TruckCoordinates> {
    @Override
    public TruckCoordinates deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(data, TruckCoordinates.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
