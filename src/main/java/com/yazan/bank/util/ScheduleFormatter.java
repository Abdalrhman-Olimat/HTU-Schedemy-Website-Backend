package com.yazan.bank.util;

import com.yazan.bank.model.Instructors;
import com.yazan.bank.model.Schedules;

import java.util.List;

public class ScheduleFormatter {

    public static String generateHtmlSchedule(Instructors instructor, List<Schedules> schedules) {
        StringBuilder html = new StringBuilder();

        html.append("<h2>Class Schedule - ").append(instructor.getName()).append("</h2>");
        html.append("<table border='1' style='border-collapse: collapse; width: 100%;'>");
        html.append("<tr>")
                .append("<th>Day</th>")
                .append("<th>Time</th>")
                .append("<th>Course</th>")
                .append("<th>Room</th>")
                .append("</tr>");

        for (Schedules s : schedules) {
            String day = (s.getTimeslot() != null && s.getTimeslot().getDay() != null)
                    ? s.getTimeslot().getDay().toString()
                    : "N/A";

            String time = (s.getTimeslot() != null && s.getTimeslot().getStartTime() != null && s.getTimeslot().getEndTime() != null)
                    ? s.getTimeslot().getStartTime() + " - " + s.getTimeslot().getEndTime()
                    : "N/A";

            String course = (s.getCourse() != null && s.getCourse().getName() != null)
                    ? s.getCourse().getName()
                    : "N/A";

            String room = (s.getRoom() != null && s.getRoom().getName() != null)
                    ? s.getRoom().getName()
                    : "N/A";

            html.append("<tr>")
                    .append("<td>").append(day).append("</td>")
                    .append("<td>").append(time).append("</td>")
                    .append("<td>").append(course).append("</td>")
                    .append("<td>").append(room).append("</td>")
                    .append("</tr>");
        }

        html.append("</table>");
        return html.toString();
    }
}
