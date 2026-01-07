package com.yazan.bank.repository;

import com.yazan.bank.model.Courses;
import com.yazan.bank.model.Department;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CoursesRepository extends JpaRepository<Courses, Long> {
    public Optional<Courses> findById(Long id);
    public Optional<Courses> findByName(String name);
    public List<Courses>findByDepartment(Department department);
}
