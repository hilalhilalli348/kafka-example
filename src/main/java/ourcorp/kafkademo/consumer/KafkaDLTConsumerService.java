package ourcorp.kafkademo.consumer;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@KafkaListener(topics = "username-changed-event-dlt", groupId = "my-group-1")
public class KafkaDLTConsumerService {

    @KafkaHandler
    public void listen(String msg) {

        System.err.println("dead letter topic " + msg);

    }

}