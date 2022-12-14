/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Checks what partitions are ready for streaming
 */
public class FindPartitionForStreamingOperation implements Operation {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionForStreamingOperation.class);

    private boolean isRequiredPublishSyncEvent = false;

    public FindPartitionForStreamingOperation() {
    }

    private TaskSyncContext takePartitionForStreaming(TaskSyncContext taskSyncContext) {

        Set<String> finishedPartitions = getFinishedPartitions(taskSyncContext);

        TaskState taskState = taskSyncContext.getCurrentTaskState();
        List<PartitionState> partitions = taskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getState().equals(PartitionStateEnum.CREATED)
                            && finishedPartitions.containsAll(partitionState.getParents())) {

                        this.isRequiredPublishSyncEvent = true;

                        LOGGER.debug("Task takes partition for streaming, taskUid: {}, partition {}",
                                taskSyncContext.getTaskUid(), partitionState.getToken());

                        return partitionState.toBuilder()
                                .state(PartitionStateEnum.READY_FOR_STREAMING)
                                .build();
                    }
                    return partitionState;
                }).collect(Collectors.toList());

        return taskSyncContext.toBuilder()
                .currentTaskState(taskState.toBuilder().partitions(partitions).build())
                .build();
    }

    private Set<String> getFinishedPartitions(TaskSyncContext taskSyncContext) {
        List<PartitionState> partitionStateList = new ArrayList<>();
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .collect(Collectors.toList()));

        return partitionStateList.stream()
                .filter(partitionState -> PartitionStateEnum.FINISHED.equals(partitionState.getState())
                        || PartitionStateEnum.REMOVED.equals(partitionState.getState()))
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return takePartitionForStreaming(taskSyncContext);
    }

}
