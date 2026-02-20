package com.example.functions;

import com.google.protobuf.util.JsonFormat;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.HistoryEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public class DatabaseRepository {
    private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer()
        .preservingProtoFieldNames()
        .omittingInsignificantWhitespace();

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public DatabaseRepository(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    public void initSchema() throws SQLException {
        String ddl = """
            CREATE TABLE IF NOT EXISTS workflow_history (
                id IDENTITY PRIMARY KEY,
                workflow_id VARCHAR(255) NOT NULL,
                run_id VARCHAR(255),
                event_id BIGINT NOT NULL,
                event_type VARCHAR(255) NOT NULL,
                event_time TIMESTAMP,
                raw_event CLOB,
                event_json CLOB,
                attempt INT,
                identity VARCHAR(512),
                activity_id VARCHAR(255),
                signal_name VARCHAR(255),
                timer_id VARCHAR(255),
                scheduled_event_id BIGINT,
                started_event_id BIGINT,
                workflow_task_completed_event_id BIGINT,
                result_json CLOB,
                failure_message CLOB,
                failure_type VARCHAR(255),
                failure_source VARCHAR(255),
                failure_non_retryable BOOLEAN,
                retry_state VARCHAR(64),
                is_failure BOOLEAN
            )
            """;

        String ddlErrors = """
            CREATE TABLE IF NOT EXISTS workflow_ingestion_errors (
                id IDENTITY PRIMARY KEY,
                workflow_id VARCHAR(255),
                run_id VARCHAR(255),
                queue_message CLOB,
                error_type VARCHAR(255),
                error_message CLOB,
                error_stacktrace CLOB,
                created_at TIMESTAMP NOT NULL
            )
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement stmt = conn.prepareStatement(ddl)) {
            stmt.execute();
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement stmt = conn.prepareStatement(ddlErrors)) {
            stmt.execute();
        }

        ensureColumns();
    }

    public int saveEvents(String workflowId, String runId, List<HistoryEvent> events) throws SQLException {
        String insert = """
            INSERT INTO workflow_history (
                workflow_id, run_id, event_id, event_type, event_time, raw_event,
                event_json, attempt, identity, activity_id, signal_name, timer_id,
                scheduled_event_id, started_event_id, workflow_task_completed_event_id,
                result_json, failure_message, failure_type, failure_source,
                failure_non_retryable, retry_state, is_failure
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement stmt = conn.prepareStatement(insert)) {
            int count = 0;
            for (HistoryEvent event : events) {
                EventMetadata metadata = EventMetadata.from(event);

                stmt.setString(1, workflowId);
                stmt.setString(2, runId);
                stmt.setLong(3, event.getEventId());
                stmt.setString(4, event.getEventType().name());
                stmt.setTimestamp(5, toTimestamp(event));
                stmt.setString(6, event.toString());
                stmt.setString(7, metadata.eventJson);
                stmt.setObject(8, metadata.attempt);
                stmt.setString(9, metadata.identity);
                stmt.setString(10, metadata.activityId);
                stmt.setString(11, metadata.signalName);
                stmt.setString(12, metadata.timerId);
                stmt.setObject(13, metadata.scheduledEventId);
                stmt.setObject(14, metadata.startedEventId);
                stmt.setObject(15, metadata.workflowTaskCompletedEventId);
                stmt.setString(16, metadata.resultJson);
                stmt.setString(17, metadata.failureMessage);
                stmt.setString(18, metadata.failureType);
                stmt.setString(19, metadata.failureSource);
                stmt.setObject(20, metadata.failureNonRetryable);
                stmt.setString(21, metadata.retryState);
                stmt.setObject(22, metadata.isFailure);
                stmt.addBatch();
                count++;
            }
            stmt.executeBatch();
            return count;
        }
    }

    public void saveIngestionFailure(String workflowId, String runId, String queueMessage, Exception exception) throws SQLException {
        String insert = """
            INSERT INTO workflow_ingestion_errors (
                workflow_id, run_id, queue_message, error_type, error_message, error_stacktrace, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement stmt = conn.prepareStatement(insert)) {
            stmt.setString(1, workflowId);
            stmt.setString(2, runId);
            stmt.setString(3, queueMessage);
            stmt.setString(4, exception == null ? null : exception.getClass().getName());
            stmt.setString(5, exception == null ? null : exception.getMessage());
            stmt.setString(6, exception == null ? null : stackTrace(exception));
            stmt.setTimestamp(7, Timestamp.from(Instant.now()));
            stmt.executeUpdate();
        }
    }

    private void ensureColumns() throws SQLException {
        String[] alters = new String[]{
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS event_json CLOB",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS attempt INT",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS identity VARCHAR(512)",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS activity_id VARCHAR(255)",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS signal_name VARCHAR(255)",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS timer_id VARCHAR(255)",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS scheduled_event_id BIGINT",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS started_event_id BIGINT",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS workflow_task_completed_event_id BIGINT",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS result_json CLOB",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS failure_message CLOB",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS failure_type VARCHAR(255)",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS failure_source VARCHAR(255)",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS failure_non_retryable BOOLEAN",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS retry_state VARCHAR(64)",
            "ALTER TABLE workflow_history ADD COLUMN IF NOT EXISTS is_failure BOOLEAN"
        };

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            for (String alter : alters) {
                try (PreparedStatement stmt = conn.prepareStatement(alter)) {
                    stmt.execute();
                }
            }
        }
    }

    private String stackTrace(Exception exception) {
        StringBuilder builder = new StringBuilder();
        builder.append(exception).append(System.lineSeparator());
        for (StackTraceElement element : exception.getStackTrace()) {
            builder.append("\tat ").append(element).append(System.lineSeparator());
        }
        return builder.toString();
    }

    private Timestamp toTimestamp(HistoryEvent event) {
        if (!event.hasEventTime()) {
            return null;
        }
        Instant instant = Instant.ofEpochSecond(event.getEventTime().getSeconds(), event.getEventTime().getNanos());
        return Timestamp.from(instant);
    }

    private static String emptyToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value;
    }

    private static Long longOrNull(long value) {
        return value == 0 ? null : value;
    }

    private static Integer intOrNull(long value) {
        return value == 0 ? null : (int) value;
    }

    private static String firstPayloadAsText(io.temporal.api.common.v1.Payloads payloads) {
        if (payloads == null || payloads.getPayloadsCount() == 0) {
            return null;
        }
        return emptyToNull(payloads.getPayloads(0).getData().toStringUtf8());
    }

    private static void fillFailure(Failure failure, EventMetadata metadata) {
        if (failure == null) {
            return;
        }

        metadata.isFailure = true;
        metadata.failureMessage = emptyToNull(failure.getMessage());
        metadata.failureSource = emptyToNull(failure.getSource());

        if (failure.hasApplicationFailureInfo()) {
            metadata.failureType = emptyToNull(failure.getApplicationFailureInfo().getType());
            metadata.failureNonRetryable = failure.getApplicationFailureInfo().getNonRetryable();
        } else if (failure.hasTimeoutFailureInfo()) {
            metadata.failureType = "TIMEOUT_" + failure.getTimeoutFailureInfo().getTimeoutType().name();
        } else if (failure.hasCanceledFailureInfo()) {
            metadata.failureType = "CANCELED";
        } else if (failure.hasTerminatedFailureInfo()) {
            metadata.failureType = "TERMINATED";
        } else if (failure.hasServerFailureInfo()) {
            metadata.failureType = "SERVER_FAILURE";
            metadata.failureNonRetryable = failure.getServerFailureInfo().getNonRetryable();
        } else if (failure.hasActivityFailureInfo()) {
            metadata.failureType = "ACTIVITY_FAILURE";
        } else if (failure.hasChildWorkflowExecutionFailureInfo()) {
            metadata.failureType = "CHILD_WORKFLOW_FAILURE";
        } else if (failure.hasApplicationFailureInfo()) {
            metadata.failureType = emptyToNull(failure.getApplicationFailureInfo().getType());
        }

        if (metadata.failureType == null) {
            metadata.failureType = "FAILURE";
        }
    }

    private static final class EventMetadata {
        private String eventJson;
        private Integer attempt;
        private String identity;
        private String activityId;
        private String signalName;
        private String timerId;
        private Long scheduledEventId;
        private Long startedEventId;
        private Long workflowTaskCompletedEventId;
        private String resultJson;
        private String failureMessage;
        private String failureType;
        private String failureSource;
        private Boolean failureNonRetryable;
        private String retryState;
        private Boolean isFailure;

        private static EventMetadata from(HistoryEvent event) {
            EventMetadata metadata = new EventMetadata();

            try {
                metadata.eventJson = JSON_PRINTER.print(event);
            } catch (Exception ignored) {
                metadata.eventJson = null;
            }

            switch (event.getEventType()) {
                case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED -> {
                    var attributes = event.getWorkflowExecutionStartedEventAttributes();
                    metadata.attempt = intOrNull(attributes.getAttempt());
                    metadata.identity = emptyToNull(attributes.getIdentity());
                }
                case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED -> {
                    var attributes = event.getWorkflowTaskScheduledEventAttributes();
                    metadata.attempt = intOrNull(attributes.getAttempt());
                }
                case EVENT_TYPE_WORKFLOW_TASK_STARTED -> {
                    var attributes = event.getWorkflowTaskStartedEventAttributes();
                    metadata.identity = emptyToNull(attributes.getIdentity());
                    metadata.scheduledEventId = longOrNull(attributes.getScheduledEventId());
                }
                case EVENT_TYPE_WORKFLOW_TASK_COMPLETED -> {
                    var attributes = event.getWorkflowTaskCompletedEventAttributes();
                    metadata.identity = emptyToNull(attributes.getIdentity());
                    metadata.scheduledEventId = longOrNull(attributes.getScheduledEventId());
                    metadata.startedEventId = longOrNull(attributes.getStartedEventId());
                }
                case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED -> {
                    var attributes = event.getActivityTaskScheduledEventAttributes();
                    metadata.activityId = emptyToNull(attributes.getActivityId());
                    metadata.workflowTaskCompletedEventId = longOrNull(attributes.getWorkflowTaskCompletedEventId());
                }
                case EVENT_TYPE_ACTIVITY_TASK_STARTED -> {
                    var attributes = event.getActivityTaskStartedEventAttributes();
                    metadata.identity = emptyToNull(attributes.getIdentity());
                    metadata.attempt = intOrNull(attributes.getAttempt());
                    metadata.scheduledEventId = longOrNull(attributes.getScheduledEventId());
                }
                case EVENT_TYPE_ACTIVITY_TASK_COMPLETED -> {
                    var attributes = event.getActivityTaskCompletedEventAttributes();
                    metadata.identity = emptyToNull(attributes.getIdentity());
                    metadata.scheduledEventId = longOrNull(attributes.getScheduledEventId());
                    metadata.startedEventId = longOrNull(attributes.getStartedEventId());
                }
                case EVENT_TYPE_ACTIVITY_TASK_FAILED -> {
                    var attributes = event.getActivityTaskFailedEventAttributes();
                    metadata.identity = emptyToNull(attributes.getIdentity());
                    metadata.scheduledEventId = longOrNull(attributes.getScheduledEventId());
                    metadata.startedEventId = longOrNull(attributes.getStartedEventId());
                    metadata.retryState = attributes.getRetryState().name();
                    fillFailure(attributes.getFailure(), metadata);
                }
                case EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT -> {
                    var attributes = event.getActivityTaskTimedOutEventAttributes();
                    metadata.scheduledEventId = longOrNull(attributes.getScheduledEventId());
                    metadata.startedEventId = longOrNull(attributes.getStartedEventId());
                    metadata.retryState = attributes.getRetryState().name();
                    fillFailure(attributes.getFailure(), metadata);
                }
                case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED -> {
                    var attributes = event.getWorkflowExecutionSignaledEventAttributes();
                    metadata.signalName = emptyToNull(attributes.getSignalName());
                    metadata.identity = emptyToNull(attributes.getIdentity());
                }
                case EVENT_TYPE_TIMER_STARTED -> {
                    var attributes = event.getTimerStartedEventAttributes();
                    metadata.timerId = emptyToNull(attributes.getTimerId());
                    metadata.workflowTaskCompletedEventId = longOrNull(attributes.getWorkflowTaskCompletedEventId());
                }
                case EVENT_TYPE_TIMER_FIRED -> {
                    var attributes = event.getTimerFiredEventAttributes();
                    metadata.timerId = emptyToNull(attributes.getTimerId());
                    metadata.startedEventId = longOrNull(attributes.getStartedEventId());
                }
                case EVENT_TYPE_TIMER_CANCELED -> {
                    var attributes = event.getTimerCanceledEventAttributes();
                    metadata.timerId = emptyToNull(attributes.getTimerId());
                    metadata.startedEventId = longOrNull(attributes.getStartedEventId());
                    metadata.identity = emptyToNull(attributes.getIdentity());
                }
                case EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED -> {
                    var attributes = event.getWorkflowExecutionCompletedEventAttributes();
                    metadata.workflowTaskCompletedEventId = longOrNull(attributes.getWorkflowTaskCompletedEventId());
                    metadata.resultJson = firstPayloadAsText(attributes.getResult());
                }
                case EVENT_TYPE_WORKFLOW_EXECUTION_FAILED -> {
                    var attributes = event.getWorkflowExecutionFailedEventAttributes();
                    metadata.workflowTaskCompletedEventId = longOrNull(attributes.getWorkflowTaskCompletedEventId());
                    metadata.retryState = attributes.getRetryState().name();
                    fillFailure(attributes.getFailure(), metadata);
                }
                case EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT -> {
                    var attributes = event.getWorkflowExecutionTimedOutEventAttributes();
                    metadata.retryState = attributes.getRetryState().name();
                    metadata.isFailure = true;
                    metadata.failureType = "WORKFLOW_TIMEOUT";
                }
                case EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED -> {
                    var attributes = event.getWorkflowExecutionTerminatedEventAttributes();
                    metadata.identity = emptyToNull(attributes.getIdentity());
                    metadata.failureMessage = emptyToNull(attributes.getReason());
                    metadata.failureType = "WORKFLOW_TERMINATED";
                    metadata.isFailure = true;
                }
                case EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED -> {
                    var attributes = event.getWorkflowExecutionCanceledEventAttributes();
                    metadata.workflowTaskCompletedEventId = longOrNull(attributes.getWorkflowTaskCompletedEventId());
                    metadata.resultJson = firstPayloadAsText(attributes.getDetails());
                    metadata.failureType = "WORKFLOW_CANCELED";
                    metadata.isFailure = true;
                }
                default -> {
                }
            }

            return metadata;
        }
    }
}
