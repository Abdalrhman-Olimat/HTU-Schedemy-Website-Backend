package com.yazan.bank.dto.response;

import com.yazan.bank.model.Courses;
import com.yazan.bank.model.Instructors;
import com.yazan.bank.model.Rooms;
import com.yazan.bank.model.TimeSlots;

public class ScheduleResponse {
    private Long id;
    private Courses course;
    private Instructors instructor;
    private Instructors ta;
    private Rooms room;
    private TimeSlots timeSlot;

    public ScheduleResponse(Courses course, Instructors instructor, Instructors ta,
                            Rooms room, TimeSlots timeSlot) {
        this.course = course;
        this.instructor = instructor;
        this.ta = ta;
        this.room = room;
        this.timeSlot = timeSlot;
    }

    public Long getScheduleId() {
        return id;
    }

    public void setScheduleId(Long scheduleId) {
        this.id = scheduleId;
    }

    public Courses getCourse() { return course; }
    public void setCourse(Courses course) { this.course = course; }

    public Instructors getInstructor() { return instructor; }
    public void setInstructor(Instructors instructor) { this.instructor = instructor; }

    public Instructors getTa() { return ta; }
    public void setTa(Instructors ta) { this.ta = ta; }

    public Rooms getRoom() { return room; }
    public void setRoom(Rooms room) { this.room = room; }

    public TimeSlots getTimeSlot() { return timeSlot; }
    public void setTimeSlot(TimeSlots timeSlot) { this.timeSlot = timeSlot; }
}
