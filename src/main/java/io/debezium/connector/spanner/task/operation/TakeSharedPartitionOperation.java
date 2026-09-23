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
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.db.model.PartitionKey;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Take partition to work, which was shared by other task
 */
public class TakeSharedPartitionOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(TakeSharedPartitionOperation.class);

    private boolean isRequiredPublishSyncEvent = false;

    private TaskSyncContext takePartition(TaskSyncContext context) {

        TaskState taskState = context.getCurrentTaskState();

        List<PartitionState> sharedPartitions = filterDuplications(findSharedPartition(context));
        Set<PartitionKey> tokens = taskState.getPartitions().stream()
                .map(PartitionState::getKey)
                .collect(Collectors.toSet());

        // Tokens currently owned (non-finished) by other tasks. Used to detect the mutable key
        // range race condition where two tasks each received a PartitionStartRecord for the same
        // destination and both created sharedPartitions entries before either could see the other.
        // If the other task has already taken the partition into its own partitions list, skip it.
        Set<PartitionKey> otherOwnedTokens = context.getTaskStates().values().stream()
                .flatMap(ts -> ts.getPartitions().stream())
                .filter(p -> !PartitionStateEnum.FINISHED.equals(p.getState())
                        && !PartitionStateEnum.REMOVED.equals(p.getState()))
                .map(PartitionState::getKey)
                .collect(Collectors.toSet());

        List<PartitionState> partitions = new ArrayList<>(taskState.getPartitions());

        sharedPartitions.forEach(partitionState -> {
            if (!tokens.contains(partitionState.getKey())) {
                if (otherOwnedTokens.contains(partitionState.getKey())) {
                    LOGGER.warn("Task {} : skipping shared partition {} — already owned by another task",
                            context.getTaskUid(), partitionState.getKey());
                }
                else {
                    partitions.add(partitionState);
                    this.isRequiredPublishSyncEvent = true;
                    LOGGER.info("Task {} : taking shared partition {}", context.getTaskUid(), partitionState);
                }
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

    private List<PartitionState> filterDuplications(List<PartitionState> partitionStates) {
        return partitionStates.stream()
                .collect(Collectors.groupingBy(PartitionState::getKey))
                .values()
                .stream()
                .flatMap(list -> list.stream().sorted().limit(1))
                .collect(Collectors.toList());
    }

}
