package com.kingshuk.streaming.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class WordCountUseCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountUseCase.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-dataflow");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("kafka-streams-word-count-input-topic");
        KGroupedStream<String, String> groupedStream = stream
                .flatMapValues(v -> List.of(v.toLowerCase().split(" ")))
                .groupBy((k, v) -> v);

        KTable<String, Long> countsTable = groupedStream.count();
        countsTable.toStream().to("kafka-streams-word-count-output-topic",
                Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        LOGGER.info("The topology is {}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
