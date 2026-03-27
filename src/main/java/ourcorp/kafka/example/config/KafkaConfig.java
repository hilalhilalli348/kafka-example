package ourcorp.kafka.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import ourcorp.kafka.example.exception.NotRetryableException;
import ourcorp.kafka.example.exception.RetryableException;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic createUserCreatedEventTopic() {
        return TopicBuilder.name("user-created-event-topic")
                .partitions(2)
                .replicas(3)
                .config("cleanup.policy", "compact,delete")


                // This configuration is set at the topic level, but it can also be set at
                // the broker level, in which case all topics will have these settings by default.
                // for DELETE
                .config("retention.ms", "604800000") // 7 days
                .config("segment.ms", "86400000")    // 1 day (segment rotation)
                .config("segment.bytes", "268435456")   // 256MB

                // for COMPACT
                .config("min.cleanable.dirty.ratio", "0.1")
                .config("delete.retention.ms", "86400000") // tombstone retention  1 day

                .config("min.insync.replicas", "1")
                .build();

    }

    @Bean
    public NewTopic createUserCreatedEventDltTopic() {
        return TopicBuilder.name("user-created-event-topic-dlt")
                .partitions(2)
                .replicas(3)

                // This configuration is set at the topic level, but it can also be set at
                // the broker level, in which case all topics will have these settings by default.
                // for DELETE
                .config("retention.ms", "604800000")
                .config("segment.ms", "86400000")
                .config("segment.bytes", "268435456")

                // for COMPACT
                .config("min.cleanable.dirty.ratio", "0.1")
                .config("delete.retention.ms", "86400000")

                .config("min.insync.replicas", "1")

                .build();
    }

    @Bean
    public DefaultErrorHandler createDefaultErrorHandler(KafkaTemplate<String, Object> template) {
        var recover = new DeadLetterPublishingRecoverer(template,
                (r, ex) -> new TopicPartition(
                        "user-created-event-topic-dlt", r.partition())
        );

        var defErrorHandler = new DefaultErrorHandler(recover, new FixedBackOff(500L, 3));
        defErrorHandler.addNotRetryableExceptions(NotRetryableException.class);
        defErrorHandler.addRetryableExceptions(RetryableException.class);

        return defErrorHandler;
    }

}
