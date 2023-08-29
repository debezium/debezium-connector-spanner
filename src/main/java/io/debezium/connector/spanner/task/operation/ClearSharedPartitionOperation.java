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
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Clear partition from the shared section of the task state,
 * after partition was picked up by another task
 */
public class ClearSharedPartitionOperation implements Operation {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClearSharedPartitionOperation.class);

    private boolean isRequiredPublishSyncEvent = false;

    private TaskSyncContext clear(TaskSyncContext taskSyncContext) {

        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        // Retrieve the tokens that are owned by other tasks.
        Set<String> otherTokens = taskSyncContext.getTaskStates().values().stream().flatMap(taskState -> taskState.getPartitions().stream()).map(PartitionState::getToken)
                .collect(Collectors.toSet());

        // Retrieve the tokens that are shared by other tasks.
        Set<PartitionState> otherSharedTokens = taskSyncContext.getTaskStates().values().stream().flatMap(taskState -> taskState.getSharedPartitions().stream())
                .collect(Collectors.toSet());

        // Retrieve the other alive tasks.
        Set<String> otherTasks = taskSyncContext.getTaskStates().values().stream().map(taskState -> taskState.getTaskUid()).collect(Collectors.toSet());

        List<PartitionState> currentSharedList = currentTaskState.getSharedPartitions().stream()
                .collect(Collectors.toList());

        List<PartitionState> finalSharedList = new ArrayList<PartitionState>();
        boolean reassignedPartition = false;

        // Filter or reassign shared partitions that are currently owned or shared to dead tasks.
        for (PartitionState sharedToken : currentSharedList) {
            // This token is owned by another task.
            if (otherTokens.contains(sharedToken.getToken())) {
                LOGGER.info("Task {}, removing token {} since it is already owned by other tasks {}", taskSyncContext.getTaskUid(), sharedToken, otherTasks);
            }

            else {
                // This token is not owned by other tasks, nor is it shared to a dead task.
                finalSharedList.add(sharedToken);
            }
        }

        if (finalSharedList.size() != currentSharedList.size() || reassignedPartition) {
            this.isRequiredPublishSyncEvent = true;
        }

        return taskSyncContext.toBuilder().currentTaskState(currentTaskState.toBuilder()
                .sharedPartitions(finalSharedList)
                .build()).build();
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return clear(taskSyncContext);
    }
}
