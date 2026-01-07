At the beginning of each academic semester at HTU University, the academic administration faces challenges in organizing the process of course assignment for instructors and teaching assistants (TAs). Managing a large number of courses, instructors, and time slots often leads to scheduling conflicts and difficulties in tracking course assignments efficiently.

To address this issue, the university decided to develop an electronic application that helps organize and manage the course registration and scheduling process in a centralized and structured manner. The application is designed around multiple pages, where each page is responsible for a specific task that supports effective management.

The application includes:

An Add Schedule page that allows adding courses to the semester schedule by specifying lecture time, course, assigned instructor, and teaching assistant.

A View Schedule page that displays the complete schedule in a clear and organized format.

A Manage Courses page to view and manage available courses.

A Manage Instructors page to manage instructor information and their assigned courses.

The development team has completed building the application. The back-end is implemented using Spring Boot, and the front-end is hosted in a separate GitHub repository.

Deployment & Infrastructure Requirements

The university plans to deploy the system as a production-ready platform and has the following requirements:

The front-end application must be deployed using AWS Amplify.

The back-end application must be deployed on AWS using EC2 instances.

The application must be accessible through a custom domain name that can be easily managed and updated.

The system must support high availability and automatic scaling to handle increased usage during peak registration periods.

The application must provide fast access to users from different geographical locations, as the university plans to open branches in multiple countries.

The system must run continuously for many years, and any service failure or abnormal behavior must be monitored and detected automatically.

When critical system events occur (such as instance failure, scaling actions, or deployment changes), the responsible technical team must be notified immediately.

The infrastructure should support safe application updates without service interruption.

Tasks

Task 1 – Architecture Design Propose an AWS-based infrastructure architecture that satisfies the above requirements. Clearly explain the services used and how they contribute to availability, scalability, global access, monitoring, and notification.

Task 2 – Implementation Implement the proposed solution by:

Deploying the front-end application using AWS Amplify.

Deploying the Spring Boot back-end application on AWS EC2.

Configuring the required AWS services to support domain management, global access, monitoring, event handling, and notifications.

Ensuring that the application can be updated without service interruption.

The implementation should follow best practices and demonstrate a clear understanding of cloud infrastructure concepts.
