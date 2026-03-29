package ourcorp.kafka.example.producer;

import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import ourcorp.kafka.example.model.event.UserCreatedEvent;
import ourcorp.kafka.example.model.request.UserCreatedRequest;

@Slf4j
@RequiredArgsConstructor
@Service
public class UserProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void createUser(UserCreatedRequest request) {
        var event = UserCreatedEvent.newBuilder().setName(request.getName()).build();
        var uuid = UUID.randomUUID().toString();

        Message<UserCreatedEvent> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, "user-created-event-topic")
                .setHeader(KafkaHeaders.KEY, uuid)
                .setHeader("X-USER-ID", uuid)
                .build();

        kafkaTemplate.send(message);
    }


}
