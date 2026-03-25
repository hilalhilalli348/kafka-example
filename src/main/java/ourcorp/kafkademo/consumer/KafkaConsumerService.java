//package ourcorp.kafkademo.consumer;
//
//import org.springframework.kafka.annotation.KafkaHandler;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//import ourcorp.kafkademo.exception.RetryableException;
//
//@Service
//@KafkaListener(topics = "username-changed-event", groupId = "my-group")
//public class KafkaConsumerService {
//
//    @KafkaHandler
//    public void listen(String msg) {
//
//        System.err.println(msg);
//        throw new RetryableException();
//
//    }
//
//}