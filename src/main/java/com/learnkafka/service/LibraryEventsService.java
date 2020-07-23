package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.Entity.LibraryEvent;
import com.learnkafka.repo.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.learnkafka.Entity.LibraryEventType.NEW;
import static com.learnkafka.Entity.LibraryEventType.UPDATE;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = mapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent : {}", libraryEvent);
        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                break;
            default:
                log.info("invalid lib event");

        }
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("successfully persisted the library event : {}", libraryEvent);
    }
}
