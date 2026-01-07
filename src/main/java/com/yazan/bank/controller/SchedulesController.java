package com.yazan.bank.controller;

import com.yazan.bank.dto.request.CreateScheduleRequest;
import com.yazan.bank.dto.response.ScheduleResponse;
import com.yazan.bank.model.Schedules;
import com.yazan.bank.model.TimeSlots;
import com.yazan.bank.service.SchedulesService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/schedules")
public class SchedulesController {
    private final SchedulesService schedulesService;

    public SchedulesController(SchedulesService schedulesService) {
        this.schedulesService = schedulesService;
    }


    @PostMapping
    public List<Schedules> addSchedule(@RequestBody List<CreateScheduleRequest> request) {
        return schedulesService.addSchedulesById(request);
    }

    @GetMapping
    public List<Schedules> getAllSchedules() {
        return schedulesService.getAllSchedules();
    }

    @GetMapping("/byDayAndTime")
    public List<Schedules> getSchedulesByDayAndTime(@RequestParam("timeSlots") Long timeSlots) {
        return schedulesService.getSchedulesByTimeSlotsId(timeSlots);
    }


    @DeleteMapping("/schedules/{id}")
    public void deleteSchedule(@RequestParam Long id) {
        schedulesService.deleteScheduleById(id);
    }

}
