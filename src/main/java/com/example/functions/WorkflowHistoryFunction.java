package com.example.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import io.temporal.api.history.v1.HistoryEvent;

import java.sql.SQLException;
import java.util.List;

public class WorkflowHistoryFunction {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @FunctionName("WorkflowHistoryCollector")
    public void run(
        @QueueTrigger(name = "message", queueName = "%QUEUE_NAME%", connection = "AzureWebJobsStorage") String message,
        final ExecutionContext context
    ) throws Exception {
        WorkflowRequest request = MAPPER.readValue(message, WorkflowRequest.class);

        if (request.getWorkflowId() == null || request.getWorkflowId().isBlank()) {
            context.getLogger().warning("Mensaje sin workflowId. Se ignora: " + message);
            return;
        }

        String temporalTarget = getEnv("TEMPORAL_TARGET", "localhost:7233");
        String temporalNamespace = getEnv("TEMPORAL_NAMESPACE", "default");
        String jdbcUrl = getEnv("H2_JDBC_URL", "jdbc:h2:file:./data/workflow-history;AUTO_SERVER=TRUE");
        String jdbcUser = getEnv("H2_USER", "sa");
        String jdbcPassword = getEnv("H2_PASSWORD", "");

        TemporalHistoryClient temporalClient = new TemporalHistoryClient();
        DatabaseRepository repository = new DatabaseRepository(jdbcUrl, jdbcUser, jdbcPassword);

        repository.initSchema();
        List<HistoryEvent> events = temporalClient.fetchHistory(
            temporalTarget,
            temporalNamespace,
            request.getWorkflowId(),
            request.getRunId()
        );

        int saved = save(repository, request.getWorkflowId(), request.getRunId(), events);

        context.getLogger().info(
            "Workflow " + request.getWorkflowId() + " procesado. Eventos recuperados=" + events.size() + ", guardados=" + saved
        );
    }

    private int save(DatabaseRepository repository, String workflowId, String runId, List<HistoryEvent> events) throws SQLException {
        if (events.isEmpty()) {
            return 0;
        }
        return repository.saveEvents(workflowId, runId, events);
    }

    private String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
