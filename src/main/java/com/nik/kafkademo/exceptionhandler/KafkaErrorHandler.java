package com.nik.kafkademo.exceptionhandler;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ContainerAwareErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.util.List;
import java.util.Objects;

public class KafkaErrorHandler implements ContainerAwareErrorHandler {

    private static final String KEY_DESERIALIZATION_ERROR_KEY = "springDeserializerExceptionKey";
    private static final String VALUE_DESERIALIZATION_ERROR_KEY = "springDeserializerExceptionValue";

    @Override
    public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {

        if (thrownException.getClass().equals(DeserializationException.class)) {
            records.stream()
                    .filter(consumerRecord -> !Objects.nonNull(consumerRecord.headers().lastHeader(KEY_DESERIALIZATION_ERROR_KEY))
                            || !Objects.nonNull(consumerRecord.headers().lastHeader(VALUE_DESERIALIZATION_ERROR_KEY)))
                    .findFirst()
                    .ifPresent(consumerRecord -> {
                        String topic = consumerRecord.topic();
                        long offset = consumerRecord.offset();
                        int partition = consumerRecord.partition();
                        TopicPartition topicPartition = new TopicPartition(topic, partition);
                        consumer.seek(topicPartition, offset + 1L);
                    });
        }
    }
}
