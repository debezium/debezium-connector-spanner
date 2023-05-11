/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static io.debezium.connector.spanner.task.LoggerUtils.debug;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Instant;
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
        LOGGER.info("Task: {}, Processed incremental answer from task {}: {}", currentContext.getTaskUid(), newMessage.getTaskUid(),
                newMessage);

        // Get the current context.
        var builder = currentContext.toBuilder();

        // Retrieve the new task from the incremental message.
        TaskState newTask = newMessage.getTaskStates().get(newMessage.getTaskUid());
        if (newTask == null) {
            LOGGER.warn("The incremental message {} did not contain the task's UID: {}", newMessage, newMessage.getTaskUid());
            return builder.build();
        }

        // We don't want to reprocess the current task's incremental message.
        if (newTask.getTaskUid().equals(currentContext.getTaskUid())) {
            return builder.build();
        }

        // We only update our internal copy of the other task's state.
        TaskState currentTask = currentContext.getTaskStates().get(newMessage.getTaskUid());

        if (currentTask == null) {
            // If the in-memory task sync event does not contain the other task, we insert
            // the new task into the task states map.
            Map<String, TaskState> currentTaskStates = new HashMap<>(currentContext.getTaskStates());
            currentTaskStates.put(newMessage.getTaskUid(), newTask);
            builder.taskStates(currentTaskStates)
                    .createdTimestamp(Long.max(currentContext.getCreatedTimestamp(),
                            newMessage.getMessageTimestamp()));
        }
        else if (newTask.getStateTimestamp() > currentTask.getStateTimestamp()) {
            Instant beforeProcessing = Instant.now();
            // Remove the task state from our map.
            Map<String, TaskState> currentTaskStates = new HashMap<>(currentContext.getTaskStates());
            currentTaskStates.remove(newMessage.getTaskUid());

            // Retrieve the updated partitions from the new task.
            List<PartitionState> newTaskPartitions = newTask.getPartitions().stream()
                    .collect(Collectors.toList());
            List<String> newTaskPartitionTokens = newTask.getPartitions().stream()
                    .map(partitionState -> partitionState.getToken())
                    .collect(Collectors.toList());

            // Retrieve the updated shared partitions from the new task.
            List<PartitionState> newTaskSharedPartitions = newTask.getSharedPartitions().stream()
                    .collect(Collectors.toList());
            List<String> newTaskSharedPartitionTokens = newTask.getSharedPartitions().stream()
                    .map(partitionState -> partitionState.getToken())
                    .collect(Collectors.toList());

            // Get a list of partition tokens that the old message contained that the new message
            // didn't.
            List<PartitionState> mergedOwnedPartitions = currentTask.getPartitions().stream()
                    .filter(partitionState -> !newTaskPartitionTokens.contains(
                            partitionState.getToken()))
                    .collect(Collectors.toList());

            // Get a list of shared partition tokens that the old message contained that the
            // new message didn't
            List<PartitionState> mergedSharedPartitions = currentTask.getSharedPartitions().stream()
                    .filter(partitionState -> !newTaskSharedPartitionTokens.contains(
                            partitionState.getToken()))
                    .collect(Collectors.toList());

            // Merge the current + new partition tokens and filter out tokens that are REMOVED.
            List<PartitionState> newOwnedPartitions = newTask.getPartitions().stream()
                    .filter(partitionState -> !partitionState.getState().equals(PartitionStateEnum.REMOVED))
                    .collect(Collectors.toList());
            mergedOwnedPartitions.addAll(newOwnedPartitions);
            List<PartitionState> newSharedPartitions = newTask.getSharedPartitions().stream()
                    .filter(partitionState -> !partitionState.getState().equals(PartitionStateEnum.REMOVED))
                    .collect(Collectors.toList());
            mergedSharedPartitions.addAll(newSharedPartitions);

            // build from the new sync context.
            TaskState finalTaskState = newTask.toBuilder().partitions(mergedOwnedPartitions)
                    .sharedPartitions(mergedSharedPartitions).build();
            currentTaskStates.put(newMessage.getTaskUid(), finalTaskState);
            builder.taskStates(currentTaskStates)
                    .createdTimestamp(Long.max(currentContext.getCreatedTimestamp(),
                            newMessage.getMessageTimestamp()));
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

        if (newTask.getTaskUid().equals(currentContext.getTaskUid())) {
            return builder.build();
        }

        // We only retrieve the task state belonging to the rebalance answer.
        TaskState currentTask = currentContext.getTaskStates().get(newMessage.getTaskUid());

        if (currentTask == null || newTask.getStateTimestamp() > currentTask.getStateTimestamp()) {
            Map<String, TaskState> taskStates = new HashMap<>(currentContext.getTaskStates());
            // Remove the task state from the map.
            taskStates.remove(newMessage.getTaskUid());
            // Insert the new task state in.
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
            // We don't update our own task state.
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
        TaskSyncContext result = builder.build();
        return result;
    }

    private static Set<String> checkDuplication(Map<String, List<PartitionState>> map) {
        return map.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }
}
