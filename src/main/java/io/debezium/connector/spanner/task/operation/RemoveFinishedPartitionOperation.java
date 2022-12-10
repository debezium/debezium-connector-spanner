/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Remove finished partition from the task state,
 * as it is not needed anymore
 */
public class RemoveFinishedPartitionOperation implements Operation {
    private static final Logger LOGGER = getLogger(RemoveFinishedPartitionOperation.class);

    private static final Duration DELETION_DELAY = Duration.ofMinutes(10);

    private boolean isRequiredPublishSyncEvent = false;

    private TaskSyncContext removeFinishedPartitions(TaskSyncContext taskSyncContext) {

        TaskState taskState = taskSyncContext.getCurrentTaskState();

        List<PartitionState> partitions = taskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getState().equals(PartitionStateEnum.FINISHED)
                            && isChildrenFinished(taskSyncContext, partitionState.getToken())
                            && partitionState.getFinishedTimestamp().toSqlTimestamp().toInstant()
                                    .plus(DELETION_DELAY).isAfter(Instant.now())) {
                        LOGGER.debug("Partition {} will be removed from task state", partitionState.getToken());
                        return null;
                    }
                    return partitionState;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (taskState.getPartitions().size() != partitions.size()) {
            this.isRequiredPublishSyncEvent = true;
        }

        return taskSyncContext.toBuilder()
                .currentTaskState(taskState.toBuilder().partitions(partitions).build())
                .build();
    }

    private boolean isChildrenFinished(TaskSyncContext taskSyncContext, String token) {
        List<PartitionState> allPartitionStates = Stream.concat(
                Stream.concat(taskSyncContext.getTaskStates().values().stream().flatMap(taskState -> taskState.getPartitions().stream()),
                        taskSyncContext.getCurrentTaskState().getPartitions().stream()),
                Stream.concat(taskSyncContext.getTaskStates().values().stream().flatMap(taskState -> taskState.getSharedPartitions().stream()),
                        taskSyncContext.getCurrentTaskState().getSharedPartitions().stream()))
                .collect(Collectors.toList());

        Set<String> children = allPartitionStates.stream()
                .filter(partitionState -> partitionState.getParents().contains(token))
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());

        return children.stream().allMatch(childToken -> {
            return allPartitionStates.stream()
                    .filter(partitionState -> childToken.equals(partitionState.getToken()))
                    .allMatch(partitionState -> PartitionStateEnum.FINISHED.equals(partitionState.getState())
                            || PartitionStateEnum.REMOVED.equals(partitionState.getState()));
        });
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return removeFinishedPartitions(taskSyncContext);
    }

}
