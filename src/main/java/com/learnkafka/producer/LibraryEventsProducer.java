package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class LibraryEventsProducer {

    private KafkaTemplate<Integer,String> kafkaTemplate;
    private ObjectMapper objectMapper;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key= libraryEvent.libraryEventId();
        var value=objectMapper.writeValueAsString(libraryEvent);

        var completableFuture = kafkaTemplate.send("library-events",key,value);

        return completableFuture.whenComplete((sendResult,throwable) ->{

            if(throwable!=null){
                handleFailure(key,value,throwable);

            }else{
                 handleSucces(key,value,sendResult);

            }

                });
    }

    private void handleSucces(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("message sent successfully for the key {} and the value {} and the partition is {}",key,value,sendResult.getRecordMetadata().partition());

    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending the message and the message is {}" ,ex.getMessage(),ex);
    }
}
