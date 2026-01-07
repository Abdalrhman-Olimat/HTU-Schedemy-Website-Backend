package com.yazan.bank.service;

import com.yazan.bank.model.Courses;
import com.yazan.bank.model.Department;
import com.yazan.bank.repository.CoursesRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;

@Service
public class CourseService {
    private final CoursesRepository coursesRepository;

    public CourseService(CoursesRepository coursesRepository) {
        this.coursesRepository = coursesRepository;
    }

    public List<Courses> getAllDepartmentCourses() {
        return coursesRepository.findAll();
    }

    public Courses createCourse(String name, Department department) {

        Optional<Courses> existing = coursesRepository.findByName(name);
        if(existing.isPresent()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Course already exists");
        }

        Courses course = new Courses(name,department);
        return coursesRepository.save(course);
    }

}
