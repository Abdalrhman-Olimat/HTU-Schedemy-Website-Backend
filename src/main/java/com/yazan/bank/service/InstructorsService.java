package com.yazan.bank.service;

import com.yazan.bank.dto.request.CreateInstructorsRequest;
import com.yazan.bank.dto.response.InstructorsResponse;
import com.yazan.bank.model.Department;
import com.yazan.bank.model.Instructors;
import com.yazan.bank.repository.InstructorsRepository;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class InstructorsService {

    private final InstructorsRepository instructorsRepository;

    public InstructorsService(InstructorsRepository instructorsRepository) {
        this.instructorsRepository = instructorsRepository;
    }

    public List<InstructorsResponse> getAllInstructors() {
        return instructorsRepository.findAll().stream().map(this::convertToDTO).collect(Collectors.toList());
    }

    public Instructors addInstructor(CreateInstructorsRequest request) {
        Optional<Instructors> existing = instructorsRepository.findByName(request.getName());
        if (existing.isPresent()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Instructor name already exists");
        }
        Instructors instructor = new Instructors();

        instructor.setName(request.getName());
        instructor.setDepartment(request.getDepartment());
        instructor.setEmail(request.getEmail());
        instructor.setJobTitle(request.getJobTitle());

        return instructorsRepository.save(instructor);
    }

    public List<Instructors> getInstructorsByDepartmentOrdered(Department primaryDepartment) {
        List<Instructors> result = new java.util.ArrayList<>();

        result.addAll(instructorsRepository.findByDepartmentAndJobTitleNot(primaryDepartment,"TA"));

        for (Department dept : Department.values()) {
            if (dept != primaryDepartment) {
                result.addAll(instructorsRepository.findByDepartmentAndJobTitleNot(dept,"TA"));
            }
        }

        return result;
    }

    public List<Instructors> getTAsByDepartmentOrdered(Department primaryDepartment,String jobTitle) {
        List<Instructors> result = new java.util.ArrayList<>();

        result.addAll(instructorsRepository.findByDepartmentAndJobTitle(primaryDepartment,jobTitle));

        for (Department dept : Department.values()) {
            if (dept != primaryDepartment) {
                result.addAll(instructorsRepository.findByDepartmentAndJobTitle(dept,jobTitle));
            }
        }
        return result;
    }

    private InstructorsResponse convertToDTO(Instructors i) {

        return new InstructorsResponse(
                i.getName(),
                i.getJobTitle(),
                i.getEmail(),
                i.getTeachingLoad(),
                i.getDepartment()
        );
    }
}