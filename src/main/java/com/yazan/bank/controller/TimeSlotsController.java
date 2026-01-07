package com.yazan.bank.controller;

import com.yazan.bank.model.TimeSlots;
import com.yazan.bank.service.TimeSlotsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("time")
public class TimeSlotsController {
    public final TimeSlotsService timeSlotsService;

    public TimeSlotsController(TimeSlotsService timeSlotsService) {
        this.timeSlotsService = timeSlotsService;
    }

    @GetMapping
    public List<TimeSlots> timeSlots() {
        return timeSlotsService.getAllTimeSlots();
    }
}
