/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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

        List<String> removedSharedTokens = new ArrayList<String>();

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

            // This token is assigned to a dead task.
            else if (!otherTasks.contains(sharedToken.getAssigneeTaskUid())) {
                LOGGER.info("Task {}, token {} is not assigned to an alive task {}", taskSyncContext.getTaskUid(), sharedToken, otherTasks);

                boolean otherTaskSharesPartition = false;
                // Check if another task has shared this partition to a valid task.
                for (PartitionState otherSharedToken : otherSharedTokens) {
                    // Another task has shared this token to an alive task.
                    if (otherSharedToken.getToken().equals(sharedToken.getToken()) &&
                            otherTasks.contains(otherSharedToken.getAssigneeTaskUid())) {
                        LOGGER.info("Task {}, removing token {} since it is shared to another alive task {}", taskSyncContext.getTaskUid(), otherSharedToken, otherTasks);
                        // We want to remove this token.
                        otherTaskSharesPartition = true;
                        break;
                    }
                }

                // Reassign this token to another task if it isn't shared to another alive task.
                if (!otherTaskSharesPartition) {
                    reassignedPartition = true;
                    Random rand = new Random(System.currentTimeMillis());
                    String[] otherTasksArray = (String[]) otherTasks.toArray();
                    String assigneeTaskUid = otherTasksArray[rand.nextInt(otherTasks.size())];
                    LOGGER.info("Task {}, reassigning token {} to another task {} since it was not previously assigned to an alive task", taskSyncContext.getTaskUid(),
                            sharedToken, assigneeTaskUid);
                    finalSharedList.add(PartitionState.builder().token(sharedToken.getToken()).assigneeTaskUid(assigneeTaskUid).build());
                }
            }
            else {
                // This token is not owned by other tasks, nor is it shared to a dead task.
                finalSharedList.add(sharedToken);
            }
        }

        if (finalSharedList.size() != currentSharedList.size() || reassignedPartition) {
            this.isRequiredPublishSyncEvent = true;
            LOGGER.info("Task cleared some shared partitions, taskUid: {}, final shared list {}, original list {}", taskSyncContext.getTaskUid(), finalSharedList,
                    currentTaskState.getSharedPartitions());
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
