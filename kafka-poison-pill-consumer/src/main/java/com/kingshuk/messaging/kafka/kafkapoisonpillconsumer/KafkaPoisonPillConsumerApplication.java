package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.LoggingErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;

import java.util.Map;

@SpringBootApplication
public class KafkaPoisonPillConsumerApplication {

    @Autowired
    private KafkaProperties kafkaProperties;

    public static void main(String[] args) {
        SpringApplication.run(KafkaPoisonPillConsumerApplication.class, args);
    }

    @Bean
    public LoggingErrorHandler errorHandler() {
        return new LoggingErrorHandler();
    }

    @Bean
    @ConditionalOnExpression("${spring.kafka.dlt.enable}")
    public SeekToCurrentErrorHandler errorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
        return new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer);
    }

    @Bean
    @ConditionalOnExpression("${spring.kafka.dlt.enable}")
    public DeadLetterPublishingRecoverer publisher() {
        DefaultKafkaProducerFactory<String, byte[]> defaultKafkaProducerFactory = new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(),
                new StringSerializer(), new ByteArraySerializer());
        KafkaTemplate<String, byte[]> bytesKafkaTemplate = new KafkaTemplate<>(defaultKafkaProducerFactory);
        return new DeadLetterPublishingRecoverer(bytesKafkaTemplate);
    }

    private ConsumerFactory<String, byte[]> bytesArrayConsumerFactory() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new ByteArrayDeserializer());
    }

    @Bean
    @ConditionalOnExpression("${spring.kafka.dlt.enable}")
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> bytesArrayListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(bytesArrayConsumerFactory());
        return factory;
    }

}
