package com.yazan.bank.repository;

import com.yazan.bank.model.Rooms;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface RoomRepository extends JpaRepository<Rooms,Long> {
    Optional<Rooms> findById(Long id);
    Optional<Rooms> findByName(String name);
}
