package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer;

import com.kingshuk.messaging.kafka.kafkapoisonpillconsumer.recoverer.DefaultErrorRecoverer;
import com.kingshuk.messaging.kafka.kafkapoisonpillconsumer.recoverer.DeserializationErrorRecoverer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonDelegatingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaPoisonPillListenerConfig {

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    private DefaultErrorRecoverer defaultErrorRecoverer;

    @Autowired
    private DeserializationErrorRecoverer deserializationErrorRecoverer;

    @Bean
    @SuppressWarnings("deprecation")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> listenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            CommonDelegatingErrorHandler delegatingErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setCommonErrorHandler(delegatingErrorHandler);
        factory.setConcurrency(3);

//        factory.setRetryTemplate(createRetryTemplate());
        return factory;
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        BackOff fixedBackOff = new FixedBackOff(1000, 1);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(defaultErrorRecoverer, fixedBackOff);
        errorHandler.setCommitRecovered(true);
        errorHandler.addNotRetryableExceptions(RuntimeException.class);
        errorHandler.addRetryableExceptions(KafkaPoisonPillException.class);
        return errorHandler;
    }

    @Bean
    public DefaultErrorHandler deserializationErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(deserializationErrorRecoverer);
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    @Bean
    public CommonDelegatingErrorHandler delegatingErrorHandler(
            DefaultErrorHandler defaultErrorHandler,
            DefaultErrorHandler deserializationErrorHandler) {
        CommonDelegatingErrorHandler delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultErrorHandler);
        Map<Class<? extends Throwable>, CommonErrorHandler> errorHandlerMap = new HashMap<>();
        errorHandlerMap.put(DeserializationException.class, deserializationErrorHandler);
        delegatingErrorHandler.setErrorHandlers(errorHandlerMap);
        return delegatingErrorHandler;
    }

    private static RetryTemplate createRetryTemplate() {
        //Set retries
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);

        Map<Class<? extends Throwable>, Boolean> errorHandlerMap = new HashMap<>();
        errorHandlerMap.put(RuntimeException.class, true);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(1, errorHandlerMap, true);

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }


}