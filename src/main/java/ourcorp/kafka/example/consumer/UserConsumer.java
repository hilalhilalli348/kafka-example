package ourcorp.kafka.example.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ourcorp.kafka.example.model.event.UserCreatedEvent;

@Slf4j
@Service
public class UserConsumer {
    @KafkaListener(topics = "user-created-event-topic", groupId = "user-group")
    public void handleUserCreated(UserCreatedEvent event) {
        log.info("event: {}", event);
    }

}