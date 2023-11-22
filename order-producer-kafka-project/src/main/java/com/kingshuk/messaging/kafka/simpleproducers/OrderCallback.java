package com.kingshuk.messaging.kafka.simpleproducers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;

public class OrderCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        System.out.printf("The message went to %d partition and %d offset%n"
                , metadata.partition(), metadata.offset());
        if(Objects.nonNull(exception)){
            exception.printStackTrace();
        }
    }
}
