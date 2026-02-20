package com.example.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import io.temporal.api.history.v1.HistoryEvent;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkflowHistoryFunction {
    private static final ObjectMapper MAPPER = JsonMapper.builder()
        .enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES)
        .build();

    @FunctionName("WorkflowHistoryCollector")
    public void run(
        @QueueTrigger(name = "message", queueName = "%QUEUE_NAME%", connection = "AzureWebJobsStorage") String message,
        final ExecutionContext context
    ) throws Exception {
        WorkflowRequest request = null;
        String jdbcUrl = getEnv("H2_JDBC_URL", "jdbc:h2:file:~/function-qeue-storage-data/workflow-history;AUTO_SERVER=TRUE");
        String jdbcUser = getEnv("H2_USER", "sa");
        String jdbcPassword = getEnv("H2_PASSWORD", "");

        try {
            context.getLogger().info("Mensaje recibido desde Queue Storage.");
            request = parseRequest(message);

            if (request.getWorkflowId() == null || request.getWorkflowId().isBlank()) {
                context.getLogger().warning("Mensaje sin workflowId. Se ignora: " + message);
                return;
            }

            context.getLogger().info("Procesando workflowId=" + request.getWorkflowId() + ", runId=" + request.getRunId());

            String temporalTarget = getEnv("TEMPORAL_TARGET", "localhost:7233");
            String temporalNamespace = getEnv("TEMPORAL_NAMESPACE", "default");

            TemporalHistoryClient temporalClient = new TemporalHistoryClient();
            DatabaseRepository repository = new DatabaseRepository(jdbcUrl, jdbcUser, jdbcPassword);

            context.getLogger().info("Inicializando esquema H2 en " + jdbcUrl);
            repository.initSchema();

            context.getLogger().info("Consultando historial en Temporal target=" + temporalTarget + ", namespace=" + temporalNamespace);
            List<HistoryEvent> events = temporalClient.fetchHistory(
                temporalTarget,
                temporalNamespace,
                request.getWorkflowId(),
                request.getRunId()
            );

            context.getLogger().info("Eventos recuperados de Temporal: " + events.size());
            int saved = save(repository, request.getWorkflowId(), request.getRunId(), events);

            context.getLogger().info(
                "Workflow " + request.getWorkflowId() + " procesado. Eventos recuperados=" + events.size() + ", guardados=" + saved
            );
        } catch (Exception ex) {
            context.getLogger().severe("Error procesando mensaje de cola: " + ex.getMessage());
            persistFailureRecord(jdbcUrl, jdbcUser, jdbcPassword, request, message, ex, context);
            throw ex;
        }
    }

    private WorkflowRequest parseRequest(String message) {
        try {
            return MAPPER.readValue(message, WorkflowRequest.class);
        } catch (Exception ignored) {
            return parseLooseRequest(message);
        }
    }

    private WorkflowRequest parseLooseRequest(String message) {
        String trimmed = message == null ? "" : message.trim();
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            throw new IllegalArgumentException("Formato de mensaje inválido: " + message);
        }

        String body = trimmed.substring(1, trimmed.length() - 1).trim();
        Map<String, String> values = new HashMap<>();
        if (!body.isEmpty()) {
            String[] pairs = body.split(",");
            for (String pair : pairs) {
                String[] keyValue = pair.split(":", 2);
                if (keyValue.length != 2) {
                    continue;
                }
                String key = stripQuotes(keyValue[0].trim());
                String value = stripQuotes(keyValue[1].trim());
                values.put(key, value);
            }
        }

        WorkflowRequest request = new WorkflowRequest();
        request.setWorkflowId(values.get("workflowId"));
        request.setRunId(values.get("runId"));
        return request;
    }

    private String stripQuotes(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    private int save(DatabaseRepository repository, String workflowId, String runId, List<HistoryEvent> events) throws SQLException {
        if (events.isEmpty()) {
            return 0;
        }
        return repository.saveEvents(workflowId, runId, events);
    }

    private void persistFailureRecord(
        String jdbcUrl,
        String jdbcUser,
        String jdbcPassword,
        WorkflowRequest request,
        String message,
        Exception exception,
        ExecutionContext context
    ) {
        try {
            DatabaseRepository repository = new DatabaseRepository(jdbcUrl, jdbcUser, jdbcPassword);
            repository.initSchema();
            repository.saveIngestionFailure(
                request == null ? null : request.getWorkflowId(),
                request == null ? null : request.getRunId(),
                message,
                exception
            );
        } catch (Exception persistenceException) {
            context.getLogger().warning("No se pudo guardar workflow_ingestion_errors: " + persistenceException.getMessage());
        }
    }

    private String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
