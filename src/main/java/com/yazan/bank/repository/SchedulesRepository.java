package com.yazan.bank.repository;

import com.yazan.bank.model.Instructors;
import com.yazan.bank.model.Rooms;
import com.yazan.bank.model.Schedules;
import com.yazan.bank.model.TimeSlots;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.yaml.snakeyaml.events.Event;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Optional;

@Repository
public interface SchedulesRepository extends JpaRepository<Schedules, Long> {
    public Optional<Schedules> findById(String title);
    public List<Schedules> findByInstructor(Instructors instructor);
    Optional<Schedules> findByRoomAndTimeslot(Rooms room, TimeSlots timeslot);
    List<Schedules> findByTimeslot_Id(Long timeSlots);
}
