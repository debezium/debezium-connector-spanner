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
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Take partition to work, which was shared by other task
 */
public class TakeSharedPartitionOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(TakeSharedPartitionOperation.class);

    private boolean isRequiredPublishSyncEvent = false;
    private List<String> newTokens = new ArrayList<String>();

    private TaskSyncContext takePartition(TaskSyncContext context) {

        TaskState taskState = context.getCurrentTaskState();

        List<PartitionState> sharedPartitions = findSharedPartition(context);
        Set<String> tokens = taskState.getPartitions().stream()
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());

        List<PartitionState> partitions = new ArrayList<>(taskState.getPartitions());

        sharedPartitions.forEach(partitionState -> {
            if (!tokens.contains(partitionState.getToken())) {
                partitions.add(partitionState);
                this.isRequiredPublishSyncEvent = true;
                newTokens.add(partitionState.getToken());

                LOGGER.info("Task {} : taking shared partition {}", context.getTaskUid(), partitionState.getToken());
            }
        });

        return context.toBuilder()
                .currentTaskState(taskState.toBuilder().partitions(partitions).build())
                .build();
    }

    private static List<PartitionState> findSharedPartition(TaskSyncContext context) {
        final String currentTaskUid = context.getTaskUid();

        return Stream.concat(context.getTaskStates().values().stream(), Stream.of(context.getCurrentTaskState()))
                .flatMap(taskState -> taskState.getSharedPartitions().stream())
                .filter(partitionState -> currentTaskUid.equals(partitionState.getAssigneeTaskUid()))
                .collect(Collectors.toList());
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return takePartition(taskSyncContext);
    }

    @Override
    public List<String> updatedOwnedPartitions() {
        return newTokens;
    }

    @Override
    public List<String> updatedSharedPartitions() {
        return Collections.emptyList();
    }

    @Override
    public List<String> removedOwnedPartitions() {
        return Collections.emptyList();
    }

    @Override
    public List<String> removedSharedPartitions() {
        return Collections.emptyList();
    }
}