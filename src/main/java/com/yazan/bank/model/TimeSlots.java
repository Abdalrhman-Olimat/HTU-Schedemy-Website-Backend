package com.yazan.bank.model;

import javax.persistence.*;
import java.sql.Time;

@Entity
@Table(name = "TimeSlots")
public class TimeSlots {

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "timeslots_seq")
  @SequenceGenerator(name = "timeslots_seq", sequenceName = "timeslots_seq", allocationSize = 1)
  private Long id;

  private Time startTime;
  private Time endTime;

  @Enumerated(EnumType.STRING)
  @Column(name = "day_type", nullable = false)
  private DayType day;

  public TimeSlots() {}

  public TimeSlots(Time startTime, Time endTime, DayType day) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.day = day;
  }

  // Getters and setters

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Time getStartTime() {
    return startTime;
  }

  public void setStartTime(Time startTime) {
    this.startTime = startTime;
  }

  public Time getEndTime() {
    return endTime;
  }

  public void setEndTime(Time endTime) {
    this.endTime = endTime;
  }

  public DayType getDay() {
    return day;
  }

  public void setDay(DayType day) {
    this.day = day;
  }
}
