package com.yazan.bank.controller;

import com.yazan.bank.dto.request.CreateInstructorsRequest;
import com.yazan.bank.dto.response.InstructorsResponse;
import com.yazan.bank.model.Department;
import com.yazan.bank.model.Instructors;
import com.yazan.bank.service.InstructorsService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/instructor")
public class InstructorController {
    private final InstructorsService instructorsService;

    public InstructorController(InstructorsService instructorsService) {
        this.instructorsService = instructorsService;
    }

    @PostMapping
    public Instructors addInstructor(@RequestBody CreateInstructorsRequest request) {
        return instructorsService.addInstructor(request);
    }

    @GetMapping
    public List<Instructors> getAllInstructors(@RequestParam(defaultValue = "COMPUTER_SCIENCE") Department departmentName) {
        return instructorsService.getInstructorsByDepartmentOrdered(departmentName);
    }

    @GetMapping("/ta")
    public List<Instructors> getAllTAs(@RequestParam(defaultValue = "COMPUTER_SCIENCE") Department departmentName) {
        return instructorsService.getTAsByDepartmentOrdered(departmentName,"TA");
    }



}
