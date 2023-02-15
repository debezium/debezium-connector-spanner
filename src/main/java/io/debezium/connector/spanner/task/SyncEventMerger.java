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
import java.util.stream.Stream;

import org.slf4j.Logger;

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

    public static TaskSyncContext merge(TaskSyncContext context, TaskSyncEvent inSync) {
        Map<String, TaskState> newTaskStatesMap = inSync.getTaskStates();
        debug(LOGGER, "merge: state before {}, \nIncoming states: {}", context, newTaskStatesMap);

        var builder = context.toBuilder();

        Set<String> updatedStatesUids = new HashSet<>();

        // We only update our internal copies of other task states from received sync event messages.
        // We do not update our own internal task state from received sync event messages, since
        // we have the most up-to-date version of our own task state.
        for (TaskState inTaskState : newTaskStatesMap.values()) {

            if (inTaskState.getTaskUid().equals(context.getTaskUid())) {
                continue;
            }

            TaskState currentTaskState = context.getTaskStates().get(inTaskState.getTaskUid());
            // We only update our internal copy of another task's state, if the state timestamp
            // in the sync event message is greater than the state timestamp of our internal
            // copy of the other task's state.
            if (currentTaskState == null || inTaskState.getStateTimestamp() > currentTaskState.getStateTimestamp()) {
                updatedStatesUids.add(inTaskState.getTaskUid());
            }
        }

        if (!updatedStatesUids.isEmpty()) {
            var oldStatesStream = context.getTaskStates().entrySet().stream()
                    .filter(e -> !updatedStatesUids.contains(e.getKey()));

            var updatedStatesStream = newTaskStatesMap.entrySet().stream()
                    .filter(e -> updatedStatesUids.contains(e.getKey()));

            Map<String, TaskState> mergedTaskStates = Stream.concat(oldStatesStream, updatedStatesStream)
                    .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            builder.taskStates(mergedTaskStates)
                    .createdTimestamp(Long.max(context.getCreatedTimestamp(), inSync.getMessageTimestamp()));

            TaskSyncContext result = builder
                    .build();

            debug(LOGGER, "merge: final state {}, \nUpdated uids: {}, epoch: {}",
                    result, updatedStatesUids, result.getRebalanceGenerationId());

            return result;
        }
        LOGGER.debug("merge: final state is not changed");

        return builder.build();
    }

    public static TaskSyncContext mergeRebalanceAnswer(TaskSyncContext context, TaskSyncEvent inSync) {
        Map<String, TaskState> newTaskStatesMap = inSync.getTaskStates();
        debug(LOGGER, "merge: state before {}, \nIncoming states: {}", context, newTaskStatesMap);

        var builder = context.toBuilder();

        Set<String> updatedStatesUids = new HashSet<>();

        TaskState publishingTask = inSync.getTaskStates().get(inSync.getTaskUid());
        if (publishingTask == null) {
            LOGGER.warn("The rebalance answer {} did not contain the task's UID: {}", inSync, inSync.getTaskUid());
            return builder.build();
        }

        TaskState rebalanceTaskState = context.getTaskStates().get(inSync.getTaskUid());
        // We only update our internal copy of another task's state, if the state timestamp
        // in the sync event message is greater than the state timestamp of our internal
        // copy of the other task's state.
        if (rebalanceTaskState == null || publishingTask.getStateTimestamp() > rebalanceTaskState.getStateTimestamp()) {
            Map<String, TaskState> taskStates = new HashMap<>(context.getTaskStates());
            taskStates.remove(inSync.getTaskUid());
            taskStates.put(inSync.getTaskUid(), publishingTask);
            builder.taskStates(taskStates)
                    .createdTimestamp(Long.max(context.getCreatedTimestamp(), inSync.getMessageTimestamp()));
            TaskSyncContext result = builder
                    .build();
            LOGGER.info("Processed rebalance answer {} from task {} for rebalance generation id {}", inSync, inSync.getTaskUid(), inSync.getRebalanceGenerationId());
            return result;
        }
        LOGGER.debug("merge: final state is not changed");

        return builder.build();
    }

    public static TaskSyncContext mergeEpochUpdate(TaskSyncContext context, TaskSyncEvent inSync) {
        Map<String, TaskState> newTaskStatesMap = inSync.getTaskStates();
        debug(LOGGER, "merge: state before {}, \nIncoming states: {}", context, newTaskStatesMap);

        var builder = context.toBuilder();

        Set<String> updatedStatesUids = new HashSet<>();

        // We only update our internal copies of other task states from received sync event messages.
        // We do not update our own internal task state from received sync event messages, since
        // we have the most up-to-date version of our own task state.
        for (TaskState inTaskState : newTaskStatesMap.values()) {

            if (inTaskState.getTaskUid().equals(context.getTaskUid())) {
                continue;
            }

            TaskState currentTaskState = context.getTaskStates().get(inTaskState.getTaskUid());
            // We only update our internal copy of another task's state, if the state timestamp
            // in the sync event message is greater than the state timestamp of our internal
            // copy of the other task's state.
            if (currentTaskState == null || inTaskState.getStateTimestamp() > currentTaskState.getStateTimestamp()) {
                updatedStatesUids.add(inTaskState.getTaskUid());
            }
        }

        if (!updatedStatesUids.isEmpty()) {
            var oldStatesStream = context.getTaskStates().entrySet().stream()
                    .filter(e -> !updatedStatesUids.contains(e.getKey()));

            var updatedStatesStream = newTaskStatesMap.entrySet().stream()
                    .filter(e -> updatedStatesUids.contains(e.getKey()));

            Map<String, TaskState> mergedTaskStates = Stream.concat(oldStatesStream, updatedStatesStream)
                    .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            builder.taskStates(mergedTaskStates)
                    .createdTimestamp(Long.max(context.getCreatedTimestamp(), inSync.getMessageTimestamp()));

            TaskSyncContext result = builder
                    .build();

            debug(LOGGER, "merge: final state {}, \nUpdated uids: {}, epoch: {}",
                    result, updatedStatesUids, result.getRebalanceGenerationId());

            return result;
        }
        LOGGER.debug("merge: final state is not changed");

        return builder.build();
    }

}
