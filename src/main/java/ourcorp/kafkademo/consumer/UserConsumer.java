package ourcorp.kafkademo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import ourcorp.kafkademo.model.event.UserCreatedEvent;

@Slf4j
@Service
@KafkaListener(topics = "outbox.event.User", groupId = "user-group")
public class UserConsumer {

    @KafkaHandler
    public void handleUserCreated(UserCreatedEvent event, @Header("eventType") String eventType) {

        log.info("eventType : {} and event: {}", eventType, event);

    }
}