/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static io.debezium.connector.spanner.task.LoggerUtils.debug;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Utility to merge incoming task states
 * with the current task state
 */
public class SyncEventMerger {
    private static final Logger LOGGER = getLogger(SyncEventMerger.class);

    private SyncEventMerger() {
    }

    // Apply the deltas from the new message onto the current task state.
    public static TaskSyncContext mergeIncrementalTaskSyncEvent(TaskSyncContext currentContext, TaskSyncEvent newMessage) {
        Map<String, TaskState> newTaskStatesMap = newMessage.getTaskStates();
        debug(LOGGER, "merge: state before {}, \nIncoming states: {}", currentContext, newTaskStatesMap);

        var builder = currentContext.toBuilder();

        TaskState newTask = newMessage.getTaskStates().get(newMessage.getTaskUid());
        if (newTask == null) {
            LOGGER.warn("The incremental message {} did not contain the task's UID: {}", newMessage, newMessage.getTaskUid());
            return builder.build();
        }

        // We only update our internal copy of the other task's state.
        TaskState currentTask = currentContext.getTaskStates().get(newMessage.getTaskUid());
        // We only update our internal copy of another task's state, if the state timestamp
        // in the sync event message is greater than the state timestamp of our internal
        // copy of the other task's state.

        if (currentTask == null || newTask.getStateTimestamp() > currentTask.getStateTimestamp()) {

            // Remove the new message's task state from our current task state.
            Map<String, TaskState> currentTaskStates = new HashMap<>(currentContext.getTaskStates());
            currentTaskStates.remove(newMessage.getTaskUid());

            // Get the updated owned partitions, newly removed owned partitions, newly shared partitions,
            // newly removed shared partitions from the sync event message.
            List<PartitionState> updatedOwnedPartitions = newTask.getPartitions().stream()
                    .filter(partitionState -> !partitionState.getState().equals(PartitionStateEnum.REMOVED))
                    .collect(Collectors.toList());
            List<PartitionState> removedOwnedPartitions = newTask.getPartitions().stream()
                    .filter(partitionState -> partitionState.getState().equals(PartitionStateEnum.REMOVED))
                    .collect(Collectors.toList());
            List<PartitionState> newSharedPartitions = newTask.getSharedPartitions().stream()
                    .filter(partitionState -> !partitionState.getState().equals(PartitionStateEnum.REMOVED))
                    .collect(Collectors.toList());
            List<PartitionState> removedSharedPartitions = newTask.getSharedPartitions().stream()
                    .filter(partitionState -> partitionState.getState().equals(PartitionStateEnum.REMOVED))
                    .collect(Collectors.toList());

            // Compute the final owned partitions for this task.
            List<PartitionState> finalOwnedPartitions = new ArrayList<>();
            for (PartitionState currentPartition : currentTask.getPartitions()) {
                // Only add the partitions from the current task sync context, if it was not newly
                // modified or removed.
                if (!removedOwnedPartitions.contains(currentPartition) &&
                        !updatedOwnedPartitions.contains(currentPartition)) {
                    finalOwnedPartitions.add(currentPartition);
                }
            }
            // Add all the newly modified partitions.
            finalOwnedPartitions.addAll(updatedOwnedPartitions);

            // Compute the final shared partitions.
            List<PartitionState> finalSharedPartitions = new ArrayList<>();
            for (PartitionState currentPartition : currentTask.getSharedPartitions()) {
                // Only add the partitions from the current task sync context, if it was not newly
                // shared or removed.
                if (!removedSharedPartitions.contains(currentPartition) &&
                        !newSharedPartitions.contains(currentPartition)) {
                    finalSharedPartitions.add(currentPartition);
                }
            }
            // Add all the newly shared partitions.
            finalSharedPartitions.addAll(newSharedPartitions);

            // build from the new sync context.
            TaskState finalTaskState = newTask.builder().partitions(finalOwnedPartitions)
                    .sharedPartitions(finalSharedPartitions).build();
            currentTaskStates.put(currentContext.getTaskUid(), finalTaskState);
            builder.taskStates(currentTaskStates)
                    .createdTimestamp(Long.max(currentContext.getCreatedTimestamp(),
                            newMessage.getMessageTimestamp()));
            TaskSyncContext result = builder
                    .build();
            LOGGER.debug("Processed incremental answer {} from task {} for " +
                    "rebalance generation id {}", newMessage, newMessage.getTaskUid(),
                    newMessage.getRebalanceGenerationId());
            return result;
        }
        LOGGER.debug("merge: final state is not changed");

        return builder.build();
    }

    // Take in the new task's snapshot.
    public static TaskSyncContext mergeRebalanceAnswer(TaskSyncContext currentContext, TaskSyncEvent newMessage) {
        Map<String, TaskState> newTaskStatesMap = newMessage.getTaskStates();
        debug(LOGGER, "merge: state before {}, \nIncoming states: {}", currentContext, newTaskStatesMap);

        var builder = currentContext.toBuilder();

        TaskState newTask = newMessage.getTaskStates().get(newMessage.getTaskUid());
        if (newTask == null) {
            LOGGER.warn("The rebalance answer {} did not contain the task's UID: {}", newMessage, newMessage.getTaskUid());
            return builder.build();
        }

        // We only retrieve the task state belonging to the rebalance answer.
        TaskState currentTask = currentContext.getTaskStates().get(newMessage.getTaskUid());

        // We only update our internal copy of another task's state, if the state timestamp
        // in the sync event message is greater than the state timestamp of our internal
        // copy of the other task's state.
        if (currentTask == null || newTask.getStateTimestamp() > currentTask.getStateTimestamp()) {
            Map<String, TaskState> taskStates = new HashMap<>(currentContext.getTaskStates());
            taskStates.remove(newMessage.getTaskUid());
            taskStates.put(newMessage.getTaskUid(), newTask);
            builder.taskStates(taskStates)
                    .createdTimestamp(Long.max(currentContext.getCreatedTimestamp(),
                            newMessage.getMessageTimestamp()));
            TaskSyncContext result = builder
                    .build();
            LOGGER.info("Processed rebalance answer {} from task {} for rebalance generation id {}", newMessage, newMessage.getTaskUid(),
                    newMessage.getRebalanceGenerationId());
            return result;
        }
        LOGGER.debug("merge: final state is not changed");

        return builder.build();
    }

    // For UPDATE_EPOCH messages, we only update a task's state if the new message contains
    // the same message for the task which had a higher state timestamp.
    public static TaskSyncContext mergeEpochUpdate(TaskSyncContext currentContext, TaskSyncEvent newMessage) {
        Map<String, TaskState> newTaskStatesMap = newMessage.getTaskStates();
        debug(LOGGER, "merge: state before {}, \nIncoming states: {}", currentContext, newTaskStatesMap);

        var builder = currentContext.toBuilder();

        Set<String> updatedStatesUids = new HashSet<>();

        // We only update our internal copies of other task states from received sync event messages.
        // We do not update our own internal task state from received sync event messages, since
        // we have the most up-to-date version of our own task state.
        for (TaskState newTaskState : newTaskStatesMap.values()) {

            if (newTaskState.getTaskUid().equals(currentContext.getTaskUid())) {
                continue;
            }

            TaskState currentTaskState = currentContext.getTaskStates().get(newTaskState.getTaskUid());
            // We only update our internal copy of another task's state, if the state timestamp
            // in the sync event message is greater than the state timestamp of our internal
            // copy of the other task's state.
            if (currentTaskState == null || newTaskState.getStateTimestamp() > currentTaskState.getStateTimestamp()) {
                updatedStatesUids.add(newTaskState.getTaskUid());
            }
        }

        if (!updatedStatesUids.isEmpty()) {
            var oldStatesStream = currentContext.getTaskStates().entrySet().stream()
                    .filter(e -> !updatedStatesUids.contains(e.getKey()));

            var updatedStatesStream = newTaskStatesMap.entrySet().stream()
                    .filter(e -> updatedStatesUids.contains(e.getKey()));

            Map<String, TaskState> mergedTaskStates = Stream.concat(oldStatesStream, updatedStatesStream)
                    .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            builder.taskStates(mergedTaskStates)
                    .createdTimestamp(Long.max(currentContext.getCreatedTimestamp(), newMessage.getMessageTimestamp()));

            TaskSyncContext result = builder
                    .build();

            debug(LOGGER, "merge: final state {}, \nUpdated uids: {}, epoch: {}",
                    result, updatedStatesUids, result.getRebalanceGenerationId());

            return result;
        }
        LOGGER.debug("merge: final state is not changed");

        return builder.build();
    }

    // Take in an entire snapshot.
    public static TaskSyncContext mergeNewEpoch(TaskSyncContext currentContext, TaskSyncEvent newMessage) {
        var builder = currentContext.toBuilder();
        Map<String, TaskState> newTaskStates = new HashMap<>(newMessage.getTaskStates());
        newTaskStates.remove(currentContext.getTaskUid());
        // When we receive NEW_EPOCH messages, we clear all task states belonging to tasks
        // other than the current task state, and replace them with entries from the
        // NEW_EPOCH message.
        builder.rebalanceState(RebalanceState.NEW_EPOCH_STARTED)
                .createdTimestamp(newMessage.getMessageTimestamp())
                .taskStates(newTaskStates)
                // update timestamp for the current task state
                .currentTaskState(currentContext.getCurrentTaskState()
                        .toBuilder()
                        .stateTimestamp(newMessage.getMessageTimestamp())
                        .build());
        return builder.build();
    }
}
