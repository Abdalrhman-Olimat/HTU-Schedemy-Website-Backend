package com.yazan.bank.dto.response;

import com.yazan.bank.model.Department;

public class InstructorsResponse {
    private Long id;
    private String name;
    private String jobTitle;
    private String email;
    private double unassignedTeachingLoad;
    private Department department;

    public InstructorsResponse(String name, String jobTitle, String email, double unassignedTeachingLoad, Department department) {
        this.name = name;
        this.jobTitle = jobTitle;
        this.email = email;
        this.unassignedTeachingLoad = unassignedTeachingLoad;
        this.department = department;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setUnassignedTeachingLoad(double unassignedTeachingLoad) {
        this.unassignedTeachingLoad = unassignedTeachingLoad;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    public void setJobTitle(String jobTitle) {
        this.jobTitle = jobTitle;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public double getUnassignedTeachingLoad() {
        return unassignedTeachingLoad;
    }

    public void setTeachingLoad(double teachingLoad) {
        this.unassignedTeachingLoad = teachingLoad;
    }

    public Department getDepartment() {
        return department;
    }

    public void setDepartment(Department department) {
        this.department = department;
    }
}