/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.ConflictResolver;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Check if the same partition is already streaming by other tasks.
 * If yes - identify only one task, which will continue streaming.
 */
public class CheckPartitionDuplicationOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckPartitionDuplicationOperation.class);

    private boolean isRequiredPublishSyncEvent = false;

    private final ChangeStream changeStream;

    public CheckPartitionDuplicationOperation(ChangeStream changeStream) {
        this.changeStream = changeStream;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {

        for (PartitionState partitionState : taskSyncContext.getCurrentTaskState().getPartitions()) {

            String token = partitionState.getToken();

            Map<String, PartitionState> taskUidPartitionState = getTasksStreamingPartition(taskSyncContext.getTaskStates().values(), token);

            if (taskUidPartitionState.isEmpty()) {
                continue;
            }

            this.isRequiredPublishSyncEvent = needToStopStreaming(taskSyncContext.getTaskUid(), taskUidPartitionState);

            if (isRequiredPublishSyncEvent) {
                taskSyncContext = stopStreaming(taskSyncContext, partitionState);
                LOGGER.debug("Stop streaming the partition: {}", token);
            }
            else {
                LOGGER.warn("Continue streaming the partition: {}", token);
            }
        }

        return taskSyncContext;
    }

    private Map<String, PartitionState> getTasksStreamingPartition(Collection<TaskState> taskStates, String token) {
        Map<String, PartitionState> result = new HashMap<>();

        for (TaskState taskState : taskStates) {
            Optional<PartitionState> partitionState = taskState.getPartitions().stream()
                    .filter(state -> state.getToken().equals(token)).findFirst();
            partitionState.ifPresent(state -> result.put(taskState.getTaskUid(), state));
        }
        return result;
    }

    private boolean needToStopStreaming(String currentTaskUid,
                                        Map<String, PartitionState> taskUidPartitionState) {

        Set<String> tasks = taskUidPartitionState.entrySet().stream()
                .filter(entry -> PartitionStateEnum.RUNNING.equals(entry.getValue().getState())
                        || PartitionStateEnum.SCHEDULED.equals(entry.getValue().getState()))
                .map(Map.Entry::getKey).collect(Collectors.toSet());

        return !ConflictResolver.hasPriority(currentTaskUid, tasks);
    }

    private TaskSyncContext stopStreaming(TaskSyncContext taskSyncContext, PartitionState state) {
        String token = state.getToken();

        changeStream.stop(token);

        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        List<PartitionState> partitions = currentTaskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getToken().equals(token)) {
                        return null;
                    }
                    return partitionState;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return taskSyncContext.toBuilder()
                .currentTaskState(currentTaskState.toBuilder().partitions(partitions).build())
                .build();
    }
}
