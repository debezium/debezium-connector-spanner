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
 * Clear partition from the shared section of the task state,
 * after partition was picked up by another task
 */
public class ClearSharedPartitionOperation implements Operation {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClearSharedPartitionOperation.class);

    private boolean isRequiredPublishSyncEvent = false;

    private TaskSyncContext clear(TaskSyncContext taskSyncContext) {

        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        // Retrieve the tokens that are owned by other tasks.
        Set<String> otherTokens = taskSyncContext.getAllTaskStates().values().stream().flatMap(taskState -> taskState.getPartitions().stream())
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());

        List<PartitionState> currentSharedList = currentTaskState.getSharedPartitions().stream()
                .collect(Collectors.toList());

        List<PartitionState> finalSharedList = new ArrayList<PartitionState>();

        // Filter or reassign shared partitions that are currently owned or shared to dead tasks.
        for (PartitionState sharedToken : currentSharedList) {
            // This token is owned by another task (it has been moved into that task's partitions
            // list, in any state including FINISHED). Only drop our sharedPartitions entry once
            // the other task has actually taken ownership — never drop it while the competing
            // task still only has a sharedPartitions claim, to avoid leaving the partition
            // unstreamed if that task crashes before it starts.
            if (otherTokens.contains(sharedToken.getToken())) {
                LOGGER.info("Task {}, removing token {} since it is already owned by other tasks", taskSyncContext.getTaskUid(), sharedToken);
            }
            else {
                // This token is not owned by other tasks, nor is it shared to a dead task.
                finalSharedList.add(sharedToken);
            }
        }

        Set<String> lowerUidActiveTokens = lowerUidActivePartitionTokens(taskSyncContext);

        List<PartitionState> currentPartitions = new ArrayList<>(currentTaskState.getPartitions());
        List<PartitionState> finalPartitions = new ArrayList<>(currentPartitions.size());
        boolean partitionsHealed = false;

        for (PartitionState p : currentPartitions) {
            if (!PartitionStateEnum.FINISHED.equals(p.getState())
                    && !PartitionStateEnum.REMOVED.equals(p.getState())
                    && lowerUidActiveTokens.contains(p.getToken())) {
                LOGGER.warn("Task {}, self-healing duplicate partition {} — a lower-UID task already owns it; marking REMOVED",
                        taskSyncContext.getTaskUid(), p.getToken());
                finalPartitions.add(p.toBuilder().state(PartitionStateEnum.REMOVED).build());
                partitionsHealed = true;
            }
            else {
                finalPartitions.add(p);
            }
        }

        if (finalSharedList.size() != currentSharedList.size() || partitionsHealed) {
            this.isRequiredPublishSyncEvent = true;
        }

        return taskSyncContext.toBuilder().currentTaskState(currentTaskState.toBuilder()
                .sharedPartitions(finalSharedList)
                .partitions(finalPartitions)
                .build()).build();
    }

    /**
     * Returns the set of partition tokens actively owned (non-FINISHED, non-REMOVED) by tasks
     * whose UID is lexicographically smaller than the current task's UID. Used to detect
     * partitions-level duplicates created by the mutable key range race condition so the
     * higher-UID task can yield.
     */
    private Set<String> lowerUidActivePartitionTokens(TaskSyncContext context) {
        String currentUid = context.getCurrentTaskState().getTaskUid();
        return context.getTaskStates().values().stream()
                .filter(ts -> ts.getTaskUid().compareTo(currentUid) < 0)
                .flatMap(ts -> ts.getPartitions().stream())
                .filter(p -> !PartitionStateEnum.FINISHED.equals(p.getState())
                        && !PartitionStateEnum.REMOVED.equals(p.getState()))
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
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
