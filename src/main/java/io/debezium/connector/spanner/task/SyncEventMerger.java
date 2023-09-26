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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;

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
            LOGGER.warn("The rebalance answer {} did not contain the task's UID: {}", newMessage, newMessage.getTaskUid());
            return builder.build();
        }

        if (newTask.getTaskUid().equals(currentContext.getTaskUid())) {
            return builder.build();
        }

        long oldPartitions = currentContext.getNumPartitions() + currentContext.getNumSharedPartitions();
        // We only retrieve the task state belonging to the rebalance answer.
        TaskState currentTask = currentContext.getTaskStates().get(newMessage.getTaskUid());

        if (currentTask == null) {
            LOGGER.debug("Task {}, The task's UID: {} not contained in current task states map", currentContext.getTaskUid(), newMessage.getTaskUid());
            return builder.build();
        }

        if (newTask.getStateTimestamp() > currentTask.getStateTimestamp()) {
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
            long newPartitions = result.getNumPartitions() + result.getNumSharedPartitions();
            if (newPartitions != oldPartitions) {
                LOGGER.debug(
                        "Task {}, processed incremental answer {}: {}, task has total partitions {}, num partitions {}, num shared partitions {}, num old partitions {}",
                        currentContext.getTaskUid(), newMessage, result.getNumPartitions() + result.getNumSharedPartitions(),
                        result.getNumPartitions(), result.getNumSharedPartitions(), oldPartitions);

            }
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

        if (newTask.getTaskUid().equals(currentContext.getTaskUid())) {
            return builder.build();
        }

        long oldPartitions = currentContext.getNumPartitions() + currentContext.getNumSharedPartitions();
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
            LOGGER.info(
                    "Task {}, Processed rebalance answer from task {} for rebalance generation id {}, task has total partitions {}, num partitions {}, num shared partitions {}, num old partitions {}",
                    currentContext.getTaskUid(), newMessage.getTaskUid(),
                    newMessage.getRebalanceGenerationId(), result.getNumPartitions() + result.getNumSharedPartitions(),
                    result.getNumPartitions(), result.getNumSharedPartitions(), oldPartitions);

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
        // do not merge if not starting initial sync / not new epoch started.
        boolean startInitialSync = currentContext.getRebalanceState() == RebalanceState.START_INITIAL_SYNC;

        if (!startInitialSync && !currentContext.getRebalanceState().equals(RebalanceState.NEW_EPOCH_STARTED)) {
            return builder.build();
        }

        // If the current task is the leader, there is no need to merge the epoch update.
        if (newMessage.getTaskUid().equals(currentContext.getTaskUid())) {
            return builder.build();
        }
        long oldPartitions = currentContext.getNumPartitions() + currentContext.getNumSharedPartitions();
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

        long newPartitions = numPartitions + numSharedPartitions;
        LOGGER.debug(
                "Task {}, updating the epoch offset from the leader's UPDATE_EPOCH message {}: {}, task has total partitions {}, num partitions {}, num shared partitions {}, num old partitions {}",
                currentContext.getTaskUid(), newMessage.getTaskUid(),
                newMessage.getEpochOffset(), numPartitions + numSharedPartitions, numPartitions, numSharedPartitions, oldPartitions);

        return result;
    }

    public static TaskSyncContext mergeNewEpoch(TaskSyncContext currentContext, TaskSyncEvent inSync) {
        var builder = currentContext.toBuilder();

        // If the current task is the leader, there is no need to merge the new epoch message.
        if (inSync.getTaskUid().equals(currentContext.getTaskUid())) {
            return builder.build();
        }

        // Check that there is no preexisting partition duplication in the task.
        boolean foundDuplication = false;
        if (currentContext.checkDuplication(false, "NEW_EPOCH")) {
            foundDuplication = true;
        }

        long oldPartitions = currentContext.getNumPartitions() + currentContext.getNumSharedPartitions();

        Map<String, TaskState> newTaskStates = new HashMap<>(inSync.getTaskStates());
        newTaskStates.remove(currentContext.getTaskUid());

        boolean startInitialSync = currentContext.getRebalanceState() == RebalanceState.START_INITIAL_SYNC;
        if (!startInitialSync) {
            Set<String> allNewEpochTasks = inSync.getTaskStates().values().stream().map(taskState -> taskState.getTaskUid()).collect(Collectors.toSet());
            if (!allNewEpochTasks.contains(currentContext.getTaskUid())) {
                LOGGER.warn(
                        "Task {} - Received new epoch message , but leader {} did not include the task in the new epoch message with rebalance ID {} with tasks {}, probably just initialized, throw exception",
                        currentContext.getTaskUid(), inSync.getTaskUid(), inSync.getRebalanceGenerationId(),
                        inSync.getTaskStates().keySet().stream().collect(Collectors.toList()));
            }
            else if (inSync.getRebalanceGenerationId() < currentContext.getReceivedRebalanceGenerationId()) {
                LOGGER.warn(
                        "Task {} - Received new epoch message from {} , but the new epoch message had rebalance generation ID {} while the latest rebalance generation ID is {}",
                        currentContext.getTaskUid(), inSync.getTaskUid(), inSync.getRebalanceGenerationId(),
                        currentContext.getReceivedRebalanceGenerationId());

            }
            else {
                LOGGER.info("Task {}, updating the rebalance state to NEW_EPOCH_STARTED {}: {}", currentContext.getTaskUid(), inSync.getTaskUid(),
                        inSync.getRebalanceGenerationId());
                builder.rebalanceState(RebalanceState.NEW_EPOCH_STARTED);
            }
        }

        TaskState.TaskStateBuilder currentTaskBuilder = currentContext.getCurrentTaskState().toBuilder();
        LOGGER.info("Task {}, updating the rebalance generation ID and epoch offset from the leader new epoch {}: {}, {}", currentContext.getTaskUid(),
                inSync.getTaskUid(),
                inSync.getRebalanceGenerationId(), inSync.getEpochOffset());
        currentTaskBuilder.rebalanceGenerationId(inSync.getRebalanceGenerationId());

        builder
                .createdTimestamp(inSync.getMessageTimestamp())
                .rebalanceGenerationId(inSync.getRebalanceGenerationId())
                // Update the epoch offset from the leader's epoch offset.
                .epochOffsetHolder(currentContext.getEpochOffsetHolder().nextOffset(inSync.getEpochOffset()))
                .taskStates(newTaskStates)
                .currentTaskState(currentTaskBuilder.build());
        TaskSyncContext result = builder.build();

        long newPartitions = result.getNumPartitions() + result.getNumSharedPartitions();
        LOGGER.info("Task {}, processed new epoch message {}: {}, task has total partitions {}, num partitions {}, num shared partitions {}, num old partitions {}",
                currentContext.getTaskUid(), inSync.getTaskUid(),
                inSync.getEpochOffset(), result.getNumPartitions() + result.getNumSharedPartitions(),
                result.getNumPartitions(), result.getNumSharedPartitions(), oldPartitions);

        // Check that there is no partition duplication after processing the new epoch message.
        if (!foundDuplication && result.checkDuplication(true, "NEW_EPOCH")) {
            LOGGER.debug("Task {}, duplication exists after processing new epoch, old context {}", result.getTaskUid(), currentContext);
            LOGGER.debug("Task {}, duplication exists after processing new epoch, new message {}", result.getTaskUid(), inSync);
            LOGGER.debug("Task {}, duplication exists after processing new epoch, resulting context {}", result.getTaskUid(), result);
        }

        return result;
    }
}
