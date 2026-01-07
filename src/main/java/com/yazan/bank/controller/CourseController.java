package com.yazan.bank.controller;

import com.yazan.bank.model.Courses;
import com.yazan.bank.model.Department;
import com.yazan.bank.repository.CoursesRepository;
import com.yazan.bank.service.CourseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/courses")
public class CourseController {
    @Autowired
    private final CourseService courseService;

    public CourseController(CourseService courseService) {
        this.courseService = courseService;
    }

    @PostMapping
    public Courses addCourse(@RequestParam String courseName,@RequestParam Department departmentName) {
       return courseService.createCourse(courseName,departmentName);
    }

    @GetMapping
    public List<Courses> getAllCourses() {
        return courseService.getAllDepartmentCourses();
    }


}