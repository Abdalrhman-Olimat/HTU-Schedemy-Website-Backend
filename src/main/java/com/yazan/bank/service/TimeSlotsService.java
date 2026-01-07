package com.yazan.bank.service;

import com.yazan.bank.dto.response.InstructorsResponse;
import com.yazan.bank.model.Instructors;
import com.yazan.bank.model.TimeSlots;
import com.yazan.bank.repository.TimeSlotRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class TimeSlotsService {
    TimeSlotRepository timeSlotRepository;
    public TimeSlotsService(TimeSlotRepository timeSlotRepository) {
        this.timeSlotRepository = timeSlotRepository;
    }

    public List<TimeSlots> getAllTimeSlots(){
        return timeSlotRepository.findAll();
    }
}
