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
                    if (partitionState.getState().equals(PartitionStateEnum.CREATED)) {
                        boolean takePartitionForStreaming = false;
                        LOGGER.debug("Task sees partition with CREATED state, task Uid {}, partition {}", taskSyncContext.getTaskUid(), partitionState);
                        if (finishedPartitions.containsAll(partitionState.getParents())) {
                            takePartitionForStreaming = true;
                            LOGGER.info("Task takes partition for streaming, taskUid: {}, partition {}",
                                    taskSyncContext.getTaskUid(), partitionState.getToken());

                        }
                        else if (!atLeastOneParentExists(taskSyncContext, partitionState.getParents())) {
                            LOGGER.info("Task takes partition for streaming, since parents no longer exist, taskUid: {}, partition {}, parents {}",
                                    taskSyncContext.getTaskUid(), partitionState.getToken(), partitionState.getParents());
                            takePartitionForStreaming = true;
                        }

                        if (takePartitionForStreaming) {
                            this.isRequiredPublishSyncEvent = true;

                            return partitionState.toBuilder()
                                    .state(PartitionStateEnum.READY_FOR_STREAMING)
                                    .build();
                        }
                        else {
                            return partitionState;
                        }
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

    private boolean atLeastOneParentExists(TaskSyncContext taskSyncContext, Set<String> parents) {
        List<PartitionState> partitionStateList = new ArrayList<>();
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .collect(Collectors.toList()));

        Set<String> ownedPartitions = partitionStateList.stream()
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
        for (String parent : parents) {
            if (ownedPartitions.contains(parent)) {
                return true;
            }
        }
        return false;
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
