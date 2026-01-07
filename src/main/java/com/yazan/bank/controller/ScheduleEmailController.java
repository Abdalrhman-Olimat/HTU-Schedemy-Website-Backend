package com.yazan.bank.controller;

import com.yazan.bank.model.Instructors;
import com.yazan.bank.model.Schedules;
import com.yazan.bank.repository.InstructorsRepository;
import com.yazan.bank.repository.SchedulesRepository;
import com.yazan.bank.service.EmailService;
import com.yazan.bank.util.ScheduleFormatter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.mail.MessagingException;
import java.util.*;

@RestController
@RequestMapping("/email")
public class ScheduleEmailController {

    @Autowired
    private EmailService emailService;

    @Autowired
    private InstructorsRepository instructorRepo;

    @Autowired
    private SchedulesRepository scheduleRepo;

    @PostMapping()
    public ResponseEntity<?> sendSchedulesToInstructors(@RequestBody List<Long> instructorIds) {
        List<String> results = new ArrayList<>();

        for (Long id : instructorIds) {
            Optional<Instructors> optionalInstructor = instructorRepo.findById(id);
            if (optionalInstructor.isEmpty()) {
                results.add("Instructor with ID " + id + " not found.");
                continue;
            }

            Instructors instructor = optionalInstructor.get();
            List<Schedules> schedules = scheduleRepo.findByInstructor(instructor);
            String html = ScheduleFormatter.generateHtmlSchedule(instructor, schedules);

            try {
                emailService.sendScheduleEmail(instructor.getEmail(), "Your Teaching Schedule", html);
                results.add("Schedule sent to " + instructor.getEmail());
            } catch (MessagingException e) {
                results.add("Failed to send to " + instructor.getEmail() + ": " + e.getMessage());
            }
        }

        return ResponseEntity.ok(results);
    }
}
