package ourcorp.kafka.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

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
    public NewTopic createUsernameChangedEventTopic() {
        return TopicBuilder.name("username-changed-event-topic")
                .partitions(2)
                .replicas(3)
                .config("cleanup.policy", "compact,delete")
                .config("retention.ms", "604800000")
                .config("segment.ms", "86400000")
                .config("segment.bytes", "268435456")
                .config("min.cleanable.dirty.ratio", "0.1")
                .config("delete.retention.ms", "86400000")
                .config("min.insync.replicas", "1")
                .build();

    }


// If you don’t need custom topic configuration, you don’t have to create this DLT topic manually.
// @RetryableTopic will auto-create the necessary retry and DLT topics with default settings.
//    @Bean
//    public NewTopic createUserCreatedEventDltTopic() {
//        return TopicBuilder.name("user-created-event-topic-dlt")
//                .partitions(2)
//                .replicas(3)
//
//                // This configuration is set at the topic level, but it can also be set at
//                // the broker level, in which case all topics will have these settings by default.
//                // for DELETE
//                .config("retention.ms", "604800000")
//                .config("segment.ms", "86400000")
//                .config("segment.bytes", "268435456")
//
//                // for COMPACT
//                .config("min.cleanable.dirty.ratio", "0.1")
//                .config("delete.retention.ms", "86400000")
//
//                .config("min.insync.replicas", "1")
//
//                .build();
//    }


    // ===============================
// Programmatic / Imperative Non-Blocking Retry Configuration
// Use this if you prefer not to use @RetryableTopic annotation
// ===============================

//    @Bean
//    public RetryTopicConfiguration retryTopicConfig(KafkaTemplate<String, Object> template) {
//        // RetryTopicConfigurationBuilder allows us to define retry topics programmatically
//        return RetryTopicConfigurationBuilder
//                .newInstance()
//
//                .retryOn(RetryableException.class)
//
//                // Total attempts including the first attempt (main consumer + retries)
//                .maxAttempts(4)
//
//                // Initial backoff delay (in milliseconds) before first retry
//                .fixedBackOff(200L)
//
//                // Optional: exponential backoff configuration
//                .exponentialBackoff(200, 3, 2000) // delay * multiplier for subsequent retries
//
//                // Exceptions that should trigger retry
//                .retryOn(RetryableException.class)
//
//                // Dead-letter topic suffix if all retries fail
//                .dltSuffix("-dlt")
//
//                // Create the retry configuration using the KafkaTemplate
//                .create(template);
//    }


// If you use @RetryableTopic, you do NOT need to define this DefaultErrorHandler.
// This DefaultErrorHandler is meant for **blocking retries**, where the consumer thread
// retries processing the message synchronously with a fixed backoff.
// @RetryableTopic handles **non-blocking, topic-based retries** asynchronously,
// so defining this bean is optional and not required in that case.
//
// @Bean
// public DefaultErrorHandler createDefaultErrorHandler(KafkaTemplate<String, Object> template) {
//     var recover = new DeadLetterPublishingRecoverer(template,
//             (r, ex) -> new TopicPartition(
//                     "user-created-event-topic-dlt", r.partition())
//     );
//
//     var defErrorHandler = new DefaultErrorHandler(recover, new FixedBackOff(500L, 3));
//     defErrorHandler.addNotRetryableExceptions(NotRetryableException.class);
//     defErrorHandler.addRetryableExceptions(RetryableException.class);
//
//     return defErrorHandler;
// }

}
