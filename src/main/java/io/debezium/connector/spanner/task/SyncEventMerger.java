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

import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
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

        boolean foundDuplication = false;
        if (context.checkDuplication(true, " Old Process message " + inSync.getMessageType().toString())) {
            foundDuplication = true;
        }
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

        if (inSync.getMessageType() == MessageTypeEnum.UPDATE_EPOCH && !inTaskState.getTaskUid().equals(context.getTaskUid())) {
            LOGGER.info("Task {}, updating the epoch offset from the leader update epoch {}: {}", context.getTaskUid(), inSync.getTaskUid(),
                    inSync.getEpochOffset());
            builder.epochOffsetHolder(context.getEpochOffsetHolder().nextOffset(inSync.getEpochOffset()));
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

            if (!foundDuplication) {
                if (result.checkDuplication(true, "New Process message " + inSync.getMessageType().toString())) {
                    LOGGER.info("Task {} found duplication after processing {}", context.getTaskUid(), inSync);
                    LOGGER.info("Task {} final message {}", context.getTaskUid(), result);
                }
            }

            return result;
        }
        LOGGER.debug("merge: final state is not changed");

        return builder.build();
    }

    public static TaskSyncContext mergeNewEpoch(TaskSyncContext currentContext, TaskSyncEvent inSync) {
        var builder = currentContext.toBuilder();
        Set<String> allNewEpochTasks = inSync.getTaskStates().values().stream().map(taskState -> taskState.getTaskUid()).collect(Collectors.toSet());
        boolean start_initial_sync = currentContext.getRebalanceState() == RebalanceState.START_INITIAL_SYNC;
        if (!start_initial_sync && !allNewEpochTasks.contains(currentContext.getTaskUid())) {
            LOGGER.warn(
                    "Task {} - Received new epoch message , but leader did not include the task in the new epoch message {}, throwing exception",
                    currentContext.getTaskUid(), allNewEpochTasks);

            throw new IllegalStateException("New epoch message does not contain task state " + currentContext.getTaskUid());
        }

        boolean foundDuplication = false;
        if (currentContext.checkDuplication(true, "Merge new epoch")) {
            foundDuplication = true;
        }

        // If the current task is the leader, there is no need to merge the epoch update.
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
        if (!foundDuplication && result.checkDuplication(true, "NEW EPOCH")) {
            LOGGER.warn("Task {}, duplication exists after processing new epoch, old context {}", result.getTaskUid(), currentContext);
            LOGGER.warn("Task {}, duplication exists after processing new epoch, new message {}", result.getTaskUid(), inSync);
            LOGGER.warn("Task {}, duplication exists after processing new epoch, resulting context {}", result.getTaskUid(), result);
        }

        return result;
    }

}
