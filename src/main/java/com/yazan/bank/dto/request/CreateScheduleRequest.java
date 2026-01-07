package com.yazan.bank.dto.request;

import com.yazan.bank.model.Courses;
import com.yazan.bank.model.Instructors;
import com.yazan.bank.model.Rooms;
import com.yazan.bank.model.TimeSlots;

public class CreateScheduleRequest {

    private Long id;
    private Long courseId;
    private Long instructorId;
    private Long taId;
    private Long roomId;
    private Long timeSlotId;

    public Long getCourseId() {
        return courseId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setCourseId(Long courseId) {
        this.courseId = courseId;
    }

    public Long getInstructorId() {
        return instructorId;
    }

    public void setInstructorId(Long instructorId) {
        this.instructorId = instructorId;
    }

    public Long getTaId() {
        return taId;
    }

    public void setTaId(Long taId) {
        this.taId = taId;
    }

    public Long getRoomId() {
        return roomId;
    }

    public void setRoomId(Long roomId) {
        this.roomId = roomId;
    }

    public Long getTimeSlotId() {
        return timeSlotId;
    }

    public void setTimeSlotId(Long timeSlotId) {
        this.timeSlotId = timeSlotId;
    }
}
