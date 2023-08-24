/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static io.debezium.connector.spanner.task.LoggerUtils.debug;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;

import io.debezium.DebeziumException;
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
        LOGGER.info("Task {} merging {}", currentContext.getTaskUid(), newMessage);

        Map<String, TaskState> newTaskStatesMap = newMessage.getTaskStates();
        debug(LOGGER, "merge: state before {}, \nIncoming states: {}", currentContext, newTaskStatesMap);

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
            // If the in-memory task sync event does not contain the other task, we don't do
            // anything here. We assume that the task was not included in the new epoch message.
            LOGGER.warn("The current task state map for {} did not contain the message's UID: {}, ignoring message {}", currentContext.getTaskUid(),
                    newMessage.getTaskUid(), newMessage);
            return builder.build();
        }
        else if (newTask.getStateTimestamp() > currentTask.getStateTimestamp()) {
            // Remove the task state from our map.
            Map<String, TaskState> currentTaskStates = new HashMap<>(currentContext.getTaskStates());
            currentTaskStates.remove(newMessage.getTaskUid());

            // Retrieve the updated partitions from the new task.
            List<String> newTaskPartitionTokens = newTask.getPartitions().stream()
                    .map(partitionState -> partitionState.getToken())
                    .collect(Collectors.toList());

            // Retrieve the updated shared partitions from the new task.
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
        TaskSyncContext newTaskSyncContext = builder
                .build();

        return newTaskSyncContext;
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
        // If the current task is the leader, there is no need to merge the epoch update.
        if (newMessage.getTaskUid().equals(currentContext.getTaskUid())) {
            return builder.build();
        }
        builder.epochOffsetHolder(currentContext.getEpochOffsetHolder().nextOffset(newMessage.getEpochOffset()));

        TaskState.TaskStateBuilder currentTaskBuilder = currentContext.getCurrentTaskState().toBuilder();
        if (RebalanceState.START_INITIAL_SYNC.equals(currentContext.getRebalanceState())) {
            // Update the rebalance generation ID for both the task and the overall context
            // in case we are still processing from previous states and haven't connected to
            // the rebalance topic yet.
            LOGGER.info("Task {}, updating the rebalance generation ID from the leader epoch update {}: {}", currentContext.getTaskUid(), newMessage.getTaskUid(),
                    newMessage.getRebalanceGenerationId());
            builder.rebalanceGenerationId(newMessage.getRebalanceGenerationId());
            currentTaskBuilder.rebalanceGenerationId(newMessage.getRebalanceGenerationId());
        }

        // Only retrieve the current task states where the task UID is included inside the UPDATE_EPOCH message.
        Set<String> updateEpochTaskUids = newTaskStatesMap.keySet().stream().collect(Collectors.toSet());
        Map<String, TaskState> currentTaskStates = currentContext.getTaskStates();

        Map<String, TaskState> filteredTaskStates = new HashMap<String, TaskState>();
        for (Map.Entry<String, TaskState> currentTaskState : currentTaskStates.entrySet()) {
            if (!updateEpochTaskUids.contains(currentTaskState.getKey())) {
                LOGGER.info("Task {}, removing task state {} since it is not included in the UPDATE_EPOCH message {}", currentContext.getTaskUid(),
                        currentTaskState.getKey(), updateEpochTaskUids);
                continue;
            }
            filteredTaskStates.put(currentTaskState.getKey(), currentTaskState.getValue());
        }

        Set<String> updatedStatesUids = new HashSet<>();

        // We only update our internal copies of other task states from received sync event messages.
        // We do not update our own internal task state from received sync event messages, since
        // we have the most up-to-date version of our own task state.
        for (TaskState newTaskState : newTaskStatesMap.values()) {
            // We don't update our own task state.
            if (newTaskState.getTaskUid().equals(currentContext.getTaskUid())) {
                continue;
            }

            TaskState currentTaskState = filteredTaskStates.get(newTaskState.getTaskUid());
            // We only update our internal copy of another task's state, if the state timestamp
            // in the sync event message is greater than the state timestamp of our internal
            // copy of the other task's state.
            if (currentTaskState == null || newTaskState.getStateTimestamp() > currentTaskState.getStateTimestamp()) {
                updatedStatesUids.add(newTaskState.getTaskUid());
            }
        }

        var oldStatesStream = filteredTaskStates.entrySet().stream()
                .filter(e -> !updatedStatesUids.contains(e.getKey()));

        var updatedStatesStream = newTaskStatesMap.entrySet().stream()
                .filter(e -> updatedStatesUids.contains(e.getKey()));

        Map<String, TaskState> mergedTaskStates = Stream.concat(oldStatesStream, updatedStatesStream)
                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        TaskSyncContext result = builder
                .taskStates(mergedTaskStates)
                .createdTimestamp(Long.max(currentContext.getCreatedTimestamp(), newMessage.getMessageTimestamp()))
                .currentTaskState(currentTaskBuilder.build())
                .epochOffsetHolder(currentContext.getEpochOffsetHolder().nextOffset(newMessage.getEpochOffset()))
                .build();

        int numPartitions = result.getNumPartitions();

        int numSharedPartitions = result.getNumSharedPartitions();

        LOGGER.info("Task {}, updating the epoch offset from the leader's UPDATE_EPOCH message {}: {}, num partitions {}, num shared partitions {}",
                currentContext.getTaskUid(), newMessage.getTaskUid(),
                newMessage.getEpochOffset(), numPartitions, numSharedPartitions);

        return result;
    }

    public static TaskSyncContext mergeNewEpoch(TaskSyncContext currentContext, TaskSyncEvent inSync) {
        var builder = currentContext.toBuilder();
        Set<String> allNewEpochTasks = inSync.getTaskStates().values().stream().map(taskState -> taskState.getTaskUid()).collect(Collectors.toSet());
        boolean start_initial_sync = currentContext.getRebalanceState() == RebalanceState.START_INITIAL_SYNC;
        if (!start_initial_sync && !allNewEpochTasks.contains(currentContext.getTaskUid())) {
            LOGGER.warn(
                    "Task {} - Received new epoch message , but leader did not include the task in the new epoch message, throwing exception",
                    currentContext.getTaskUid());

            throw new DebeziumException("New epoch message does not contain task state " + currentContext.getTaskUid());
        }

        // Check that there is no preexisting partition duplication in the task.
        boolean foundDuplication = false;
        if (currentContext.checkDuplication(false, "NEW_EPOCH")) {
            foundDuplication = true;
        }

        // If the current task is the leader, there is no need to merge the new epoch message.
        if (inSync.getTaskUid().equals(currentContext.getTaskUid())) {
            return builder.build();
        }

        Map<String, TaskState> newTaskStates = new HashMap<>(inSync.getTaskStates());
        newTaskStates.remove(currentContext.getTaskUid());

        TaskState.TaskStateBuilder currentTaskBuilder = currentContext.getCurrentTaskState().toBuilder();

        if (RebalanceState.START_INITIAL_SYNC.equals(currentContext.getRebalanceState())) {
            // Update the rebalance generation ID for both the task and the overall context
            // in case we are still processing from previous states and haven't connected to
            // the rebalance topic yet. We don't want to update the state to NEW_EPOCH_STARTED
            // here.
            LOGGER.info("Task {}, updating the rebalance generation ID from the leader new epoch {}: {}", currentContext.getTaskUid(), inSync.getTaskUid(),
                    inSync.getRebalanceGenerationId());
            builder.rebalanceGenerationId(inSync.getRebalanceGenerationId());
            currentTaskBuilder.rebalanceGenerationId(inSync.getRebalanceGenerationId());
        }
        else {
            // Update the state to NEW_EPOCH_STARTED if we have received a new epoch message during
            // normal task execution.
            builder.rebalanceState(RebalanceState.NEW_EPOCH_STARTED);
        }

        LOGGER.info("Task {}, updating the epoch offset from the leader new epoch {}: {}", currentContext.getTaskUid(), inSync.getTaskUid(),
                inSync.getEpochOffset());
        builder
                .createdTimestamp(inSync.getMessageTimestamp())
                // Update the epoch offset from the leader's epoch offset.
                .epochOffsetHolder(currentContext.getEpochOffsetHolder().nextOffset(inSync.getEpochOffset()))
                .taskStates(newTaskStates)
                .currentTaskState(currentTaskBuilder.build());
        TaskSyncContext result = builder.build();

        // Check that there is no partition duplication after processing the new epoch message.
        if (!foundDuplication && result.checkDuplication(true, "NEW_EPOCH")) {
            LOGGER.debug("Task {}, duplication exists after processing new epoch, old context {}", result.getTaskUid(), currentContext);
            LOGGER.debug("Task {}, duplication exists after processing new epoch, new message {}", result.getTaskUid(), inSync);
            LOGGER.debug("Task {}, duplication exists after processing new epoch, resulting context {}", result.getTaskUid(), result);
        }

        return result;
    }
}
