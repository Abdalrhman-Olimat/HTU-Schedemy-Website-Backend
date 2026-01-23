package com.yazan.bank.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.HashMap;
import java.util.Map;

@Service
public class SqsNotificationService {

    private static final Logger logger = LoggerFactory.getLogger(SqsNotificationService.class);

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;

    @Value("${aws.sqs.queue.url:}")
    private String queueUrl;

    @Value("${aws.sqs.enabled:false}")
    private boolean sqsEnabled;

    public SqsNotificationService(SqsClient sqsClient) {
        this.sqsClient = sqsClient;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Send a schedule update notification to SQS
     * @param event The event type (e.g., "SCHEDULE_CREATE", "SCHEDULE_UPDATE", "SCHEDULE_DELETE")
     * @param scheduleId The ID of the schedule that was modified
     * @param courseId The ID of the course (optional)
     * @param courseName The name of the course (optional)
     */
    public void sendScheduleNotification(String event, Long scheduleId, Long courseId, String courseName) {
        if (!sqsEnabled || queueUrl == null || queueUrl.isEmpty()) {
            logger.info("SQS is disabled or queue URL not configured. Skipping notification for event: {}", event);
            return;
        }

        try {
            Map<String, Object> message = new HashMap<>();
            message.put("event", event);
            message.put("scheduleId", scheduleId);
            message.put("timestamp", System.currentTimeMillis());
            
            if (courseId != null) {
                message.put("courseId", courseId);
            }
            if (courseName != null) {
                message.put("courseName", courseName);
            }

            String messageBody = objectMapper.writeValueAsString(message);

            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build();

            SendMessageResponse response = sqsClient.sendMessage(sendMsgRequest);
            
            logger.info("SQS message sent successfully. MessageId: {}, Event: {}, ScheduleId: {}", 
                    response.messageId(), event, scheduleId);

        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize SQS message for scheduleId: {}", scheduleId, e);
        } catch (SqsException e) {
            logger.error("Failed to send SQS message for scheduleId: {}. Error: {}", 
                    scheduleId, e.awsErrorDetails().errorMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error sending SQS notification for scheduleId: {}", scheduleId, e);
        }
    }

    /**
     * Send a batch schedule update notification
     * @param event The event type
     * @param scheduleCount Number of schedules affected
     */
    public void sendBatchScheduleNotification(String event, int scheduleCount) {
        if (!sqsEnabled || queueUrl == null || queueUrl.isEmpty()) {
            logger.info("SQS is disabled or queue URL not configured. Skipping batch notification.");
            return;
        }

        try {
            Map<String, Object> message = new HashMap<>();
            message.put("event", event);
            message.put("scheduleCount", scheduleCount);
            message.put("timestamp", System.currentTimeMillis());

            String messageBody = objectMapper.writeValueAsString(message);

            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build();

            SendMessageResponse response = sqsClient.sendMessage(sendMsgRequest);
            
            logger.info("SQS batch notification sent successfully. MessageId: {}, Event: {}, Count: {}", 
                    response.messageId(), event, scheduleCount);

        } catch (Exception e) {
            logger.error("Failed to send batch SQS notification", e);
        }
    }
}
