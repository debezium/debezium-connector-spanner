/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.PartitionFactory;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Check what partitions are ready to
 * and schedule them for streaming
 */
public class TakePartitionForStreamingOperation implements Operation {
    private static final Logger LOGGER = LoggerFactory.getLogger(TakePartitionForStreamingOperation.class);

    private final ChangeStream changeStream;
    private final PartitionFactory partitionFactory;

    private boolean isRequiredPublishSyncEvent = false;

    public TakePartitionForStreamingOperation(ChangeStream changeStream, PartitionFactory partitionFactory) {
        this.changeStream = changeStream;
        this.partitionFactory = partitionFactory;
    }

    private TaskSyncContext takePartitionForStreaming(TaskSyncContext taskSyncContext) {

        TaskState taskState = taskSyncContext.getCurrentTaskState();

        List<PartitionState> toStreaming = taskState.getPartitions().stream()
                .filter(partitionState -> partitionState.getState().equals(PartitionStateEnum.READY_FOR_STREAMING))
                .collect(Collectors.toList());
        if (!toStreaming.isEmpty()) {
            LOGGER.info("Task {}, ready for streaming partitions {}", taskSyncContext.getTaskUid(), toStreaming);
        }

        Set<String> toSchedule = new HashSet<>();

        toStreaming.forEach(partitionState -> {
            if (this.submitPartition(partitionState)) {
                toSchedule.add(partitionState.getToken());
            }
            else {
                LOGGER.info("Task {}, failed to submit partition {} with state {}", taskSyncContext.getTaskUid(), partitionState, taskSyncContext.getRebalanceState());
            }
        });

        List<PartitionState> partitions = taskState.getPartitions().stream()
                .map(partitionState -> {
                    if (toSchedule.contains(partitionState.getToken())) {
                        return partitionState.toBuilder()
                                .state(PartitionStateEnum.SCHEDULED)
                                .build();
                    }
                    return partitionState;
                })
                .collect(Collectors.toList());

        isRequiredPublishSyncEvent = !toSchedule.isEmpty();
        if (isRequiredPublishSyncEvent) {
            LOGGER.info("Task scheduled {} partitions, taskUid: {}", toSchedule, taskSyncContext.getTaskUid());
        }

        return taskSyncContext.toBuilder()
                .currentTaskState(taskState.toBuilder().partitions(partitions).build())
                .build();
    }

    private boolean submitPartition(PartitionState partitionState) {

        Partition partition = partitionFactory.getPartition(partitionState);

        return changeStream.submitPartition(partition);
    }

    private TaskSyncContext removeAlreadyStreamingPartitions(TaskSyncContext taskSyncContext) {
        TaskState taskState = taskSyncContext.getCurrentTaskState();

        List<PartitionState> partitions = taskSyncContext.getCurrentTaskState().getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getState().equals(PartitionStateEnum.READY_FOR_STREAMING) &&
                            isPartitionStreamingAlready(taskSyncContext.getTaskStates().values(), partitionState.getToken(), taskState.getTaskUid())) {
                        LOGGER.info("Removing streaming partition {} with state {} since partition is already streaming", partitionState.getToken(),
                                partitionState.getState());
                        return null;
                    }
                    return partitionState;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return taskSyncContext.toBuilder()
                .currentTaskState(taskState.toBuilder().partitions(partitions).build())
                .build();
    }

    private boolean isPartitionStreamingAlready(Collection<TaskState> taskStates, String token, String taskUid) {
        boolean isPartitionStreamingAlready = taskStates.stream().flatMap(taskState -> taskState.getPartitions().stream())
                .filter(partitionState -> partitionState.getToken().equals(token))
                .anyMatch(partitionState -> partitionState.getState().equals(PartitionStateEnum.SCHEDULED)
                        || partitionState.getState().equals(PartitionStateEnum.RUNNING)
                        || partitionState.getState().equals(PartitionStateEnum.FINISHED)
                        || partitionState.getState().equals(PartitionStateEnum.REMOVED));
        return isPartitionStreamingAlready;
    }

    private boolean isPartition(Collection<TaskState> taskStates, String token) {
        return taskStates.stream().flatMap(taskState -> taskState.getPartitions().stream())
                .filter(partitionState -> partitionState.getToken().equals(token))
                .anyMatch(partitionState -> partitionState.getState().equals(PartitionStateEnum.SCHEDULED)
                        || partitionState.getState().equals(PartitionStateEnum.RUNNING)
                        || partitionState.getState().equals(PartitionStateEnum.FINISHED)
                        || partitionState.getState().equals(PartitionStateEnum.REMOVED));
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        taskSyncContext = removeAlreadyStreamingPartitions(taskSyncContext);
        TaskSyncContext tookPartitionForStreaming = takePartitionForStreaming(taskSyncContext);
        return tookPartitionForStreaming;
    }

}
