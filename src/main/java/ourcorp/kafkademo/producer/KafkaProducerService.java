package ourcorp.kafkademo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Uuid;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        var key = Uuid.randomUuid().toString();
        kafkaTemplate.send("username-changed-event", key, message)
                .whenComplete((res, ex) -> {
                    if (ex == null) {
                        log.info("message sent successfully");
                    }
                });
    }
}