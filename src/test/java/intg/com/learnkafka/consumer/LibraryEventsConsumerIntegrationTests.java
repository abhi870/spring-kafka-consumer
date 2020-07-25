package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.Entity.Book;
import com.learnkafka.Entity.LibraryEvent;
import com.learnkafka.Entity.LibraryEventType;
import com.learnkafka.repo.LibraryEventRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"lib-test3"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
, "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTests {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    ObjectMapper mapper;

    @BeforeEach
    void setUp(){
        for (MessageListenerContainer messageListenerContainer:
                kafkaListenerEndpointRegistry.getAllListenerContainers()
             ) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown(){
        libraryEventRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = "{\"libraryEventType\":\"NEW\",\"libraryEventId\":39,\"book\":{\"bookId\":99,\"bookName\":\"someones BOOK\",\"bookAuthor\":\"Rupesh dugaje\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = libraryEventRepository.findAll();
        assert libraryEvents.size() ==1 ;
        libraryEvents.forEach(libraryEvent -> {
            assertEquals(99, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = "{\"libraryEventType\":\"NEW\",\"libraryEventId\":null,\"book\":{\"bookId\":99,\"bookName\":\"someones BOOK\",\"bookAuthor\":\"Rupesh dugaje\"}}";
        LibraryEvent libraryEvent = mapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);

        Book updatedBook = Book.builder().bookId(99).bookName("awesome book").bookAuthor("abhishek p").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = mapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        //when
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        //then
//        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
//        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent persistedLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();

        assertEquals("awesome book", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void publishInvalidIdUpdateLibraryEvent() throws InterruptedException, JsonProcessingException {

        NoSuchElementException noSuchElementException = assertThrows(NoSuchElementException.class,()->{
            String json = "{\"libraryEventType\":\"NEW\",\"libraryEventId\":null,\"book\":{\"bookId\":99,\"bookName\":\"someones BOOK\",\"bookAuthor\":\"Rupesh dugaje\"}}";
            LibraryEvent libraryEvent = mapper.readValue(json, LibraryEvent.class);
            libraryEvent.getBook().setLibraryEvent(libraryEvent);
            libraryEventRepository.save(libraryEvent);

            Book updatedBook = Book.builder().bookId(99).bookName("awesome book").bookAuthor("abhishek p").build();
            libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
            libraryEvent.setBook(updatedBook);
            libraryEvent.setLibraryEventId(343);
            String updatedJson = mapper.writeValueAsString(libraryEvent);
            kafkaTemplate.sendDefault(343, updatedJson).get();

            //when
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(5, TimeUnit.SECONDS);

            //then
//        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
//        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

            LibraryEvent persistedLibraryEvent = libraryEventRepository.findById(343).get();

        });

    assertTrue(noSuchElementException.getMessage().contains("No value present"));

    }

    @Test
    void publishNullIdUpdateLibraryEvent() throws InterruptedException, JsonProcessingException {

        InvalidDataAccessApiUsageException invalidDataAccessApiUsageException = assertThrows(InvalidDataAccessApiUsageException.class,()->{
            String json = "{\"libraryEventType\":\"NEW\",\"libraryEventId\":null,\"book\":{\"bookId\":99,\"bookName\":\"someones BOOK\",\"bookAuthor\":\"Rupesh dugaje\"}}";
            LibraryEvent libraryEvent = mapper.readValue(json, LibraryEvent.class);
            libraryEvent.getBook().setLibraryEvent(libraryEvent);
            libraryEventRepository.save(libraryEvent);

            Book updatedBook = Book.builder().bookId(99).bookName("awesome book").bookAuthor("abhishek p").build();
            libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
            libraryEvent.setBook(updatedBook);
            libraryEvent.setLibraryEventId(null);
            String updatedJson = mapper.writeValueAsString(libraryEvent);
            kafkaTemplate.sendDefault(null, updatedJson).get();

            //when
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(5, TimeUnit.SECONDS);

            //then
//        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
//        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

            LibraryEvent persistedLibraryEvent = libraryEventRepository.findById(null).get();

        });

        assertTrue(invalidDataAccessApiUsageException.getMessage().contains("The given id must not be null"));

    }

    @Test
    void checkRetryPolicyWithNullLibEventId() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventType\":\"UPDATE\",\"libraryEventId\":null,\"book\":{\"bookId\":99,\"bookName\":\"The Awesome BOOK\",\"bookAuthor\":\"Rupesh dugaje\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(7)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(7)).processLibraryEvent(isA(ConsumerRecord.class));

    }
}
