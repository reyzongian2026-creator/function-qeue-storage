package com.example.functions;

import io.temporal.api.history.v1.HistoryEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class DatabaseRepository {
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
                raw_event CLOB
            )
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement stmt = conn.prepareStatement(ddl)) {
            stmt.execute();
        }
    }

    public int saveEvents(String workflowId, String runId, List<HistoryEvent> events) throws SQLException {
        String insert = """
            INSERT INTO workflow_history (workflow_id, run_id, event_id, event_type, event_time, raw_event)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement stmt = conn.prepareStatement(insert)) {
            int count = 0;
            for (HistoryEvent event : events) {
                stmt.setString(1, workflowId);
                stmt.setString(2, runId);
                stmt.setLong(3, event.getEventId());
                stmt.setString(4, event.getEventType().name());
                stmt.setTimestamp(5, event.hasEventTime() ? new java.sql.Timestamp(event.getEventTime().getSeconds() * 1000) : null);
                stmt.setString(6, event.toString());
                stmt.addBatch();
                count++;
            }
            stmt.executeBatch();
            return count;
        }
    }
}
