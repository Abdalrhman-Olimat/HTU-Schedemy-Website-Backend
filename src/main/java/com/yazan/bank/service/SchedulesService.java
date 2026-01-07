package com.yazan.bank.service;

import com.yazan.bank.dto.request.CreateScheduleRequest;
import com.yazan.bank.dto.response.InstructorsResponse;
import com.yazan.bank.dto.response.ScheduleResponse;
import com.yazan.bank.model.*;
import com.yazan.bank.repository.*;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class SchedulesService {

    private final SchedulesRepository scheduleRepository;
    private final CoursesRepository coursesRepository;
    private final InstructorsRepository instructorsRepository;
    private final RoomRepository roomsRepository;
    private final TimeSlotRepository timeSlotsRepository;

    public SchedulesService(SchedulesRepository scheduleRepository,
                            CoursesRepository coursesRepository,
                            InstructorsRepository instructorsRepository,
                            RoomRepository roomsRepository,
                            TimeSlotRepository timeSlotsRepository) {
        this.scheduleRepository = scheduleRepository;
        this.coursesRepository = coursesRepository;
        this.instructorsRepository = instructorsRepository;
        this.roomsRepository = roomsRepository;
        this.timeSlotsRepository = timeSlotsRepository;
    }
    @Transactional
    public List<Schedules> addSchedulesById(List<CreateScheduleRequest> requestList) {
        List<Schedules> schedules = new ArrayList<>();

        for (CreateScheduleRequest request : requestList) {
            Courses course = null;
            if (request.getCourseId() != null) {
                course = coursesRepository.findById(request.getCourseId()).orElse(null);
            }

            Instructors newInstructor  = null;
            if (request.getInstructorId() != null) {
                newInstructor  = instructorsRepository.findById(request.getInstructorId()).orElse(null);
            }

            Instructors ta = null;
            if (request.getTaId() != null) {
                ta = instructorsRepository.findById(request.getTaId()).orElse(null);
            }

            Rooms room = null;
            if (request.getRoomId() != null) {
                room = roomsRepository.findById(request.getRoomId()).orElse(null);
            }

            TimeSlots timeSlot = null;
            if (request.getTimeSlotId() != null) {
                timeSlot = timeSlotsRepository.findById(request.getTimeSlotId()).orElse(null);
            }

            Schedules schedule = null;

            if (request.getId() != null) {
                schedule = scheduleRepository.findById(request.getId()).orElse(null);
            }

            if (schedule == null && room != null && timeSlot != null) {
                schedule = scheduleRepository.findByRoomAndTimeslot(room, timeSlot).orElse(null);
            }

            if (schedule == null) {
                schedule = new Schedules();
            }

            Instructors oldInstructor = schedule.getInstructor();

            schedule.setCourse(course);
            schedule.setInstructor(newInstructor );
            schedule.setTa(ta);
            schedule.setRoom(room);
            schedule.setTimeslot(timeSlot);


            if ((oldInstructor == null && newInstructor != null) ||
                            (oldInstructor != null && newInstructor == null) ||
                            (oldInstructor != null && newInstructor != null && !oldInstructor.getId().equals(newInstructor.getId()))
            ) {
                if (newInstructor != null) {
                    instructorsRepository.decreaseUnassignedTeachingLoad(newInstructor.getId());
                }
                if (oldInstructor != null) {
                    instructorsRepository.increaseUnassignedTeachingLoad(oldInstructor.getId());
                }
            }


            schedules.add(schedule);

        }

        return scheduleRepository.saveAll(schedules);
    }



    private ScheduleResponse convertToDTO(Schedules s) {
        Long id= s.getId();
        Courses course = s.getCourse() != null ? s.getCourse() : null;
        Instructors instructor = s.getInstructor() != null ? s.getInstructor() : null;
        Instructors ta = s.getTa() != null ? s.getTa() : null;
        Rooms room = s.getRoom() != null ? s.getRoom() : null;
        TimeSlots timeSlot = s.getTimeslot() != null ? s.getTimeslot() : null;

        return new ScheduleResponse(
                course,
                instructor,
                ta,
                room,
                timeSlot
        );
    }



    public List<Schedules> getAllSchedules() {
        return scheduleRepository.findAll();
                /*.stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());*/
    }

    public void deleteScheduleById(Long id) {
        if (!scheduleRepository.existsById(id)) {
            throw new RuntimeException("Schedule not found with id: " + id);
        }
        scheduleRepository.deleteById(id);
    }


    public List<Schedules> getSchedulesByTimeSlotsId(Long timeSlots) {
        return scheduleRepository.findByTimeslot_Id(timeSlots).stream().collect(Collectors.toList());
    }

    }