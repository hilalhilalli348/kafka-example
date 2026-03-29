package ourcorp.kafka.example.config;

import java.util.Map;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

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
    public DefaultErrorHandler createDefaultErrorHandler(KafkaProperties properties) {
        // 1. Create a non-Avro producer configuration for DLT.
        // We use StringSerializer for keys and ByteArraySerializer for values.
        // This ensures that corrupted or non-Avro messages can be sent to DLT
        // without triggering Schema Registry validation errors.
        Map<String, Object> config = properties.buildProducerProperties(null);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        DefaultKafkaProducerFactory<String, Object> dltPF = new DefaultKafkaProducerFactory<>(config);
        KafkaTemplate<String, Object> dltTemplate = new KafkaTemplate<>(dltPF);

        // 2. Configure the recoverer to use the specialized dltTemplate.
        // Using a raw ByteArraySerializer allows us to "bypass" the Avro schema checks
        // when moving a poison pill message to the Dead Letter Topic.
        var recover = new DeadLetterPublishingRecoverer(dltTemplate,
                (r, ex) -> new TopicPartition("user-created-event-topic-dlt", r.partition())
        );

        // 3. Initialize the ErrorHandler with 2 retries and a 500ms backoff.
        var defErrorHandler = new DefaultErrorHandler(recover, new FixedBackOff(500L, 2));

        // 4. Mark Deserialization and Serialization exceptions as non-retryable.
        // If a message has an "Unknown magic byte", retrying won't fix it.
        // It should be moved to DLT immediately to prevent blocking the consumer.
        defErrorHandler.addNotRetryableExceptions(DeserializationException.class);
        defErrorHandler.addNotRetryableExceptions(SerializationException.class);

        return defErrorHandler;
    }

}
