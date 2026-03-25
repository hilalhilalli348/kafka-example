package ourcorp.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import ourcorp.kafkademo.exception.NonRetryableException;
import ourcorp.kafkademo.exception.RetryableException;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic createTopic() {
        return TopicBuilder.name("username-changed-event")
                .partitions(1)
                .replicas(1)
                .config("min.insync.replicas", "1")
                .build();
    }

    @Bean
    NewTopic createDltTopic() {
        return TopicBuilder.name("username-changed-event-dlt")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);


        var ex = new FixedBackOff(1000L, 3);

        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(recoverer, ex);
        defaultErrorHandler.addRetryableExceptions(RetryableException.class);
        defaultErrorHandler.addNotRetryableExceptions(NonRetryableException.class);

        return defaultErrorHandler;
    }


}
