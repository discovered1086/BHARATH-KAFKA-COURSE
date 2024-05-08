package com.kingshuk.streaming.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@SuppressWarnings("java:S2095")
public class DataFlowStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataFlowStream.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dataflow");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("bharath-kafka-streams-input-topic");
        stream.foreach((k, v) -> LOGGER.info("The key: {}, value: {}", k, v));
        stream.filter((k, v) -> v.contains("token"))
                .map((k, v) -> KeyValue.pair(k, v.toUpperCase()))
                .to("bharath-kafka-streams-output-topic");

        Topology topology = builder.build();
        LOGGER.info("The topology is {}", topology.describe());

//        try(KafkaStreams streams = new KafkaStreams(topology, properties)){
//            streams.start();
//        }

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
