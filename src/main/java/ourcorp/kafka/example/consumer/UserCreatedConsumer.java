package ourcorp.kafka.example.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import ourcorp.kafka.example.exception.RetryableException;
import ourcorp.kafka.example.model.event.UserCreatedEvent;

@Slf4j
@Component
@RetryableTopic(
        // Total attempts: 1 main attempt + 3 retries
        attempts = "4",
        backoff = @Backoff(
                // Initial delay before the first retry
                delay = 200,
                // Retry delays: 200ms -> 600ms -> 1800ms
                multiplier = 3
        ),
        // Creates retry topics by adding the delay value to the main topic name
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE,
        // Retry only for RetryableException
        include = {RetryableException.class},
        // Number of partitions for retry and DLT topics
        numPartitions = "2",
        // Replication factor for retry and DLT topics
        replicationFactor = "3"
)
@KafkaListener(topics = "user-created-event-topic", groupId = "user-created-group")
public class UserCreatedConsumer {

    @KafkaHandler
    public void handleUserCreated(UserCreatedEvent event) {
        log.info("event: {}", event);
        throw new RetryableException();
    }

    @DltHandler
    public void handleDlt(UserCreatedEvent event, @Header(KafkaHeaders.ORIGINAL_OFFSET) long offset) {
        log.info("event {} , Offset: {}", event, offset);
    }

}
