/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Persists the sliding-window boundary timestamp ({@code processedTimestamp}) and the
 * boundary record sequence into the owning task's {@link PartitionState} and publishes it
 * to the sync topic so that a restarted task can resume the mutable partition from the
 * exact next unprocessed window without re-emitting boundary-duplicate records.
 */
public class WindowAdvancedOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowAdvancedOperation.class);

    private final String token;
    private final Timestamp processedTimestamp;
    private final String lastBoundaryRecordSequence;

    public WindowAdvancedOperation(String token, Timestamp processedTimestamp, String lastBoundaryRecordSequence) {
        this.token = token;
        this.processedTimestamp = processedTimestamp;
        this.lastBoundaryRecordSequence = lastBoundaryRecordSequence;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return true;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        List<PartitionState> updatedPartitions = currentTaskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getToken().equals(token)) {
                        return partitionState.toBuilder()
                                .processedTimestamp(processedTimestamp)
                                .lastBoundaryRecordSequence(lastBoundaryRecordSequence)
                                .build();
                    }
                    return partitionState;
                })
                .collect(Collectors.toList());

        LOGGER.debug("Task {}, processedTimestamp updated: partition={}, processedTimestamp={}, lastBoundaryRecordSequence={}",
                taskSyncContext.getTaskUid(), token, processedTimestamp, lastBoundaryRecordSequence);

        return taskSyncContext.toBuilder()
                .currentTaskState(currentTaskState.toBuilder().partitions(updatedPartitions).build())
                .build();
    }
}
