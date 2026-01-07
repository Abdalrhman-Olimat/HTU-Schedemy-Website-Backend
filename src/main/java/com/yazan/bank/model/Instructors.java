package com.yazan.bank.model;

import javax.persistence.*;
import java.util.HashMap;
import java.util.Map;

@Entity
@Table(name = "Instructors")
public class Instructors {

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "instructors_seq")
  @SequenceGenerator(name = "instructors_seq", sequenceName = "instructors_seq", allocationSize = 1)
  private Long id;

  @Column(unique = true)
  private String name;

  private String jobTitle;

  @Column(unique = true)
  private String email;

  private Long teachingLoad;

  @Column(name = "unassigned_teaching_load")
  private Long unassignedTeachingLoad;


  private Department department;

  private static final Map<String, Long> JOB_LOAD_MAP = Map.of(
          "Professor",6L,
          "Assistant Professor", 12L,
          "Associate Professor", 12L,
          "Lecturer", 15L,
          "Instructor", 15L,
          "Professor of Practice",12L,
          "Assistant Instructor", 15L,
          "TA",100L
  );

  public Instructors() {
  }

  public Instructors(String name, String jobTitle, String email, String department) {
    this.name = name;
    this.setJobTitle(jobTitle);
    this.email = email;
    this.department = Department.valueOf(department);
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
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

  public Long getUnassignedTeachingLoad() {
    return unassignedTeachingLoad;
  }

  public void setUnassignedTeachingLoad(Long unassignedTeachingLoad) {
    this.unassignedTeachingLoad = unassignedTeachingLoad;
  }

  public void setJobTitle(String jobTitle) {
    this.jobTitle = jobTitle;
    this.teachingLoad=JOB_LOAD_MAP.getOrDefault(jobTitle,0L);
    this.unassignedTeachingLoad = teachingLoad;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public double getTeachingLoad() {
    return teachingLoad;
  }

  public void setTeachingLoad(Long teachingLoad) {
    this.teachingLoad = teachingLoad;
  }

  public Department getDepartment() {
    return department;
  }

  public void setDepartment(Department department) {
    this.department = department;
  }
}