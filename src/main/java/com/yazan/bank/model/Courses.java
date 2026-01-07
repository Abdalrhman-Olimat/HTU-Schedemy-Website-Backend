package com.yazan.bank.model;

import javax.persistence.*;

@Entity
@Table(name = "Courses")
public class Courses {

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "courses_seq")
  @SequenceGenerator(name = "courses_seq", sequenceName = "courses_seq", allocationSize = 1)
  private Long id;
  private String name;

  private Department department;

  public Courses() {

  }

  public Courses(String name,Department department) {
    this.name = name;
    this.department = department;
  }

  public Department getDepartment() {
    return department;
  }

  public void setDepartment(Department department) {
    this.department = department;
  }

  public  Long getId(){
    return id;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public void setId(Long id) {
    this.id = id;
  }
}