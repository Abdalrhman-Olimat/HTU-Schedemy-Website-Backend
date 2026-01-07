package com.yazan.bank.repository;

import com.yazan.bank.model.TimeSlots;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.sql.Time;
import java.util.Optional;

@Repository
public interface TimeSlotRepository extends JpaRepository<TimeSlots,Long> {
    Optional<TimeSlots> findById(Long timeSlotId);
    Optional<TimeSlots> findByStartTimeAndEndTime(Time startTime, Time endTime);
}