/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static io.debezium.connector.spanner.task.LoggerUtils.debug;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    public static TaskSyncContext merge(TaskSyncContext context, TaskSyncEvent inSync) {
        Map<String, TaskState> newTaskStatesMap = inSync.getTaskStates();
        debug(LOGGER, "merge: state before {}, \nIncoming states: {}", context, newTaskStatesMap);
        boolean is_regular_msg = (context.getRebalanceState().equals(RebalanceState.NEW_EPOCH_STARTED));
        var builder = context.toBuilder();
        boolean foundDuplication = false;
        if (is_regular_msg) {
            TaskState currTaskInSyncMessage = newTaskStatesMap.get(context.getTaskUid());
            // Skip merge incoming states to current task's state if current task is dead.
            if (currTaskInSyncMessage == null) {
                LOGGER.warn("The new message task state map did not contain the task UID: {} in the current map: {}, ignoring message {}, check if task id dead.",
                        inSync.getTaskUid(), newTaskStatesMap, context.getTaskUid());
                return builder.build();
            }
            if (context.checkDuplication(false, "Before Merge Regular Msg")) {
                foundDuplication = true;
            }
        }

        Set<String> updatedStatesUids = new HashSet<>();

        // We only update our internal copies of other task states from received sync event messages.
        // We do not update our own internal task state from received sync event messages, since
        // we have the most up-to-date version of our own task state.
        for (TaskState inTaskState : newTaskStatesMap.values()) {

            if (inTaskState.getTaskUid().equals(context.getTaskUid())) {
                continue;
            }

            TaskState currentTaskState = context.getTaskStates().get(inTaskState.getTaskUid());
            // Skip merging states from dead task
            if (is_regular_msg && currentTaskState == null) {
                LOGGER.warn("The current state task map for {} did not contain the message UID: {}, ignoring message {}", context.getTaskUid(), inSync.getTaskUid(),
                        inSync);
                continue;
            }
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
            if (is_regular_msg && !foundDuplication && result.checkDuplication(false, "After Merge Regular Msg")) {
                LOGGER.warn("Duplication exists after merge regular msg on Task {},  old context {}", result.getTaskUid(), context);
                LOGGER.warn("Duplication exists after merge regular msg on Task {}, a new message {}", result.getTaskUid(), inSync);
                LOGGER.warn("Duplication exists after merge regular msg on Task {}, resulting context {}", result.getTaskUid(), result);
            }

            return result;
        }
        LOGGER.debug("merge: final state is not changed");
        return builder.build();
    }

}
