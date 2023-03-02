/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.ArrayList;
import java.util.Collections;
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
    List<String> removedSharedTokens = new ArrayList<String>();

    private TaskSyncContext clear(TaskSyncContext taskSyncContext) {

        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        Set<String> tokens = taskSyncContext.getTaskStates().values().stream().flatMap(taskState -> taskState.getPartitions().stream()).map(PartitionState::getToken)
                .collect(Collectors.toSet());

        List<PartitionState> newSharedList = currentTaskState.getSharedPartitions().stream()
                .filter(state -> !tokens.contains(state.getToken()))
                .collect(Collectors.toList());

        if (newSharedList.size() != currentTaskState.getSharedPartitions().size()) {
            this.isRequiredPublishSyncEvent = true;
            LOGGER.debug("Task cleared shared partitions, taskUid: {}", taskSyncContext.getTaskUid());
        }

        for (PartitionState partition : currentTaskState.getSharedPartitions()) {
            if (!newSharedList.contains(partition)) {
                removedSharedTokens.add(partition.getToken());
            }
        }

        return taskSyncContext.toBuilder().currentTaskState(currentTaskState.toBuilder()
                .sharedPartitions(newSharedList)
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

    @Override
    public List<String> updatedOwnedPartitions() {
        return Collections.emptyList();
    }

    @Override
    public List<String> updatedSharedPartitions() {
        return Collections.emptyList();
    };

    @Override
    public List<String> removedOwnedPartitions() {
        return Collections.emptyList();
    }

    @Override
    public List<String> removedSharedPartitions() {
        return removedSharedTokens;
    }

    @Override
    public List<String> modifiedOwnedPartitions() {
        return Collections.emptyList();
    }
}
