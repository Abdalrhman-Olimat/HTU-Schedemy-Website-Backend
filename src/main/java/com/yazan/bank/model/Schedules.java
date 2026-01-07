package com.yazan.bank.model;

import javax.persistence.*;

@Entity
@Table(name = "Schedules")
public class Schedules {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "schedules_seq")
    @SequenceGenerator(name = "schedules_seq", sequenceName = "schedules_seq", allocationSize = 1)
    @Column(name = "schedule_id")
    private Long id;

    @ManyToOne
    @JoinColumn(name = "timeslot_id", nullable = false)
    private TimeSlots timeslot;

    @ManyToOne
    @JoinColumn(name = "course_id")
    private Courses course;

    @ManyToOne
    @JoinColumn(name = "room_id", nullable = false)
    private Rooms room;

    @ManyToOne
    @JoinColumn(name = "instructor_id")
    private Instructors instructor;

    @ManyToOne
    @JoinColumn(name = "ta_id")
    private Instructors ta;

    public Schedules() {

    }

    public Schedules(TimeSlots timeslot,Rooms room){
        this.timeslot = timeslot;
        this.room = room;
        this.course = null;
        this.instructor = null ;
        this.ta = null;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public TimeSlots getTimeslot() {
        return timeslot;
    }

    public void setTimeslot(TimeSlots timeslot) {
        this.timeslot = timeslot;
    }

    public Courses getCourse() {
        return course;
    }

    public void setCourse(Courses course) {
        this.course = course;
    }

    public Rooms getRoom() {
        return room;
    }

    public void setRoom(Rooms room) {
        this.room = room;
    }

    public Instructors getInstructor() {
        return instructor;
    }

    public void setInstructor(Instructors instructor) {
        this.instructor = instructor;
    }

    public Instructors getTa() {
        return ta;
    }

    public void setTa(Instructors ta) {
        this.ta = ta;
    }
}
