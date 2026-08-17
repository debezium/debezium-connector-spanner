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

import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Updates the {@link MoveOutState} for a source partition that has processed a MoveOut event.
 * Writes the commit timestamp and destination partition tokens into the owning task's
 * {@link PartitionState} and triggers a sync-topic publish so that destination partitions
 * can observe the updated {@link PartitionState#getMoveOutState()} via the sync topic.
 */
public class MoveOutStateUpdateOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(MoveOutStateUpdateOperation.class);

    private final String token;
    private final Timestamp commitTimestamp;
    private final List<String> destinationTokens;

    public MoveOutStateUpdateOperation(String token, Timestamp commitTimestamp, List<String> destinationTokens) {
        this.token = token;
        this.commitTimestamp = commitTimestamp;
        this.destinationTokens = destinationTokens;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return true;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        MoveOutState newMoveOutState = new MoveOutState(commitTimestamp, destinationTokens);

        List<PartitionState> updatedPartitions = currentTaskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getToken().equals(token)) {
                        return partitionState.toBuilder()
                                .moveOutState(newMoveOutState)
                                .build();
                    }
                    return partitionState;
                })
                .collect(Collectors.toList());

        LOGGER.info("Task {}, MoveOut state updated: partition={}, commitTimestamp={}, destinations={}",
                taskSyncContext.getTaskUid(), token, commitTimestamp, destinationTokens);

        return taskSyncContext.toBuilder()
                .currentTaskState(currentTaskState.toBuilder().partitions(updatedPartitions).build())
                .build();
    }
}
