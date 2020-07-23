package com.learnkafka.Entity;

import lombok.*;

import javax.persistence.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class LibraryEvent {
    @Enumerated
    private LibraryEventType libraryEventType;
    @Id
    @GeneratedValue
    private Integer libraryEventId;
    @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
    @ToString.Exclude
    private Book book;
}
