package com.learnkafka.repo;

import com.learnkafka.Entity.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventRepository extends JpaRepository<LibraryEvent, Integer> {
}
