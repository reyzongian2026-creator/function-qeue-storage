package com.example.functions;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc;

import java.util.ArrayList;
import java.util.List;

public class TemporalHistoryClient {

    public List<HistoryEvent> fetchHistory(String target, String namespace, String workflowId, String runId) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
            .usePlaintext()
            .build();

        try {
            WorkflowServiceGrpc.WorkflowServiceBlockingStub stub = WorkflowServiceGrpc.newBlockingStub(channel);
            WorkflowExecution.Builder executionBuilder = WorkflowExecution.newBuilder().setWorkflowId(workflowId);
            if (runId != null && !runId.isBlank()) {
                executionBuilder.setRunId(runId);
            }

            byte[] nextPageToken = new byte[0];
            List<HistoryEvent> allEvents = new ArrayList<>();

            do {
                GetWorkflowExecutionHistoryResponse response = stub.getWorkflowExecutionHistory(
                    GetWorkflowExecutionHistoryRequest.newBuilder()
                        .setNamespace(namespace)
                        .setExecution(executionBuilder.build())
                        .setNextPageToken(com.google.protobuf.ByteString.copyFrom(nextPageToken))
                        .build()
                );

                allEvents.addAll(response.getHistory().getEventsList());
                nextPageToken = response.getNextPageToken().toByteArray();
            } while (nextPageToken.length > 0);

            return allEvents;
        } finally {
            channel.shutdownNow();
        }
    }
}
