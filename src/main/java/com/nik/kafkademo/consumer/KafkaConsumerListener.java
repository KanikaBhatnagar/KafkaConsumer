package com.nik.kafkademo.consumer;

import com.nik.kafkademo.message.Greetings;
import org.rocksdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerListener {
    private static Logger log = LoggerFactory.getLogger(KafkaConsumerListener.class);

//    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.group.id}",
//            containerFactory = "kafkaListenerContainerFactory")
//    public void consumeGreetings(@Payload Greetings greetings, @Headers MessageHeaders headers) {
//        log.info("Message from kafka: " + greetings.toString());
//    }

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.group.id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeGreetings(@Payload String greetings, @Headers MessageHeaders headers) {
        log.info("Message from kafka: " + greetings.toString());
    }
}
