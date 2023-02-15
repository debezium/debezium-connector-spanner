/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.SyncEventMetadata;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.task.state.SyncEvent;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.function.BlockingConsumer;

/**
 * Provides a logic for processing Sync Events of different types:
 * New Epoch, Rebalance Answers, Regular events
 */
public class SyncEventHandler {
    private static final Logger LOGGER = getLogger(SyncEventHandler.class);

    private final TaskSyncContextHolder taskSyncContextHolder;

    private final TaskSyncPublisher taskSyncPublisher;

    private final BlockingConsumer<TaskStateChangeEvent> eventConsumer;

    public SyncEventHandler(TaskSyncContextHolder taskSyncContextHolder, TaskSyncPublisher taskSyncPublisher,
                            BlockingConsumer<TaskStateChangeEvent> eventConsumer) {
        this.taskSyncContextHolder = taskSyncContextHolder;
        this.taskSyncPublisher = taskSyncPublisher;
        this.eventConsumer = eventConsumer;
    }

    public void updateCurrentOffset(TaskSyncEvent inSync, SyncEventMetadata metadata) {
        if (inSync == null) {
            return;
        }
        if (skipFromPreviousGeneration(inSync)) {
            return;
        }

        taskSyncContextHolder.update(oldContext -> oldContext.toBuilder()
                .currentKafkaRecordOffset(metadata.getOffset())
                .build());

        LOGGER.debug("Task {} - update task sync topic offset with {}", taskSyncContextHolder.get().getTaskUid(), metadata.getOffset());
    }

    public void processPreviousStates(TaskSyncEvent inSync, SyncEventMetadata metadata) {

        if (!RebalanceState.START_INITIAL_SYNC.equals(taskSyncContextHolder.get().getRebalanceState())) {
            return;
        }
        if (skipFromPreviousGeneration(inSync)) {
            return;
        }

        taskSyncContextHolder.lock();
        try {

            if (inSync != null) {
                LOGGER.debug("Task {} - processPreviousStates - merge", taskSyncContextHolder.get().getTaskUid());

                taskSyncContextHolder.update(context -> SyncEventMerger.merge(context, inSync));
            }

            if (metadata.isCanInitiateRebalancing()) {
                LOGGER.debug("Task {} - processPreviousStates - switch state to INITIAL_INCREMENTED_STATE_COMPLETED",
                        taskSyncContextHolder.get().getTaskUid());

                taskSyncContextHolder.update(context -> context.toBuilder()
                        .rebalanceState(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)
                        .epochOffsetHolder(context.getEpochOffsetHolder().nextOffset(context.getCurrentKafkaRecordOffset()))
                        .build());
            }
        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    public void processNewEpoch(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException {
        if (inSync == null) {
            return;
        }
        if (skipFromPreviousGeneration(inSync)) {
            return;
        }

        taskSyncContextHolder.lock();
        try {

            long currentGeneration = taskSyncContextHolder.get().getRebalanceGenerationId();
            long inGeneration = inSync.getRebalanceGenerationId();

            if (taskSyncContextHolder.get().getRebalanceState() == RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED &&
                    inSync.getMessageType() == MessageTypeEnum.NEW_EPOCH &&
                    inGeneration >= currentGeneration) { // We ignore messages with a stale rebalanceGenerationid.

                LOGGER.info("Task {} - processNewEpoch : {} metadata {}, rebalanceId: {}",
                        taskSyncContextHolder.get().getTaskUid(),
                        inSync,
                        metadata,
                        taskSyncContextHolder.get().getRebalanceGenerationId());

                Map<String, TaskState> taskStates = new HashMap<>(inSync.getTaskStates());
                taskStates.remove(taskSyncContextHolder.get().getTaskUid());
                // When we receive NEW_EPOCH messages, we clear all task states belonging to tasks
                // other than the current task state, and replace them with entries from the
                // NEW_EPOCH message.
                taskSyncContextHolder.update(context -> context.toBuilder()
                        .rebalanceState(RebalanceState.NEW_EPOCH_STARTED)
                        .createdTimestamp(inSync.getMessageTimestamp())
                        .taskStates(taskStates)

                        // update timestamp for the current task state
                        .currentTaskState(context.getCurrentTaskState()
                                .toBuilder()
                                .stateTimestamp(inSync.getMessageTimestamp())
                                .build())
                        .build());

                LOGGER.info("Task {} - Current task state after receiving newEpoch message", taskSyncContextHolder.get().getTaskUid(), taskSyncContextHolder.get());
            }
        }
        finally {
            taskSyncContextHolder.unlock();
        }

    }

    public void processUpdateEpoch(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException {
        if (inSync == null) {
            return;
        }
        if (skipFromPreviousGeneration(inSync)) {
            return;
        }

        taskSyncContextHolder.lock();
        try {

            if (!taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.NEW_EPOCH_STARTED) ||
                    inSync.getMessageType() != MessageTypeEnum.UPDATE_EPOCH) {
                // We skip all messages that are not UPDATE_EPOCH here.
                return;
            }

            LOGGER.info("Task {} - process epoch update", taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> SyncEventMerger.mergeEpochUpdate(context, inSync));

            eventConsumer.accept(new SyncEvent());
        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    public void process(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException {
        if (inSync == null) {
            return;
        }
        if (skipFromPreviousGeneration(inSync)) {
            return;
        }

        taskSyncContextHolder.lock();
        try {

            if (!taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.NEW_EPOCH_STARTED)) {
                return;
            }

            LOGGER.debug("Task {} - process sync event", taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> SyncEventMerger.merge(context, inSync));

            eventConsumer.accept(new SyncEvent());
        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    public void processRebalanceAnswer(TaskSyncEvent inSync, SyncEventMetadata metadata) {
        if (inSync == null) {
            return;
        }
        if (skipFromPreviousGeneration(inSync)) {
            return;
        }

        taskSyncContextHolder.lock();

        try {

            if (!taskSyncContextHolder.get().isLeader() ||
                    !taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)) {
                return;
            }

            LOGGER.info("Task {} - process sync event - rebalance answer {}", taskSyncContextHolder.get().getTaskUid(), inSync);

            taskSyncContextHolder.update(context -> SyncEventMerger.mergeRebalanceAnswer(context, inSync));

        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    private boolean skipFromPreviousGeneration(TaskSyncEvent inSync) {
        if (inSync != null) {
            long inGeneration = inSync.getRebalanceGenerationId();
            long currentGeneration = taskSyncContextHolder.get().getRebalanceGenerationId();

            if (inGeneration < currentGeneration) {
                LOGGER.debug("skipFromPreviousGeneration: currentGen: {}, inGen: {}, inTaskUid: {}", currentGeneration, inGeneration, inSync.getTaskUid());
                return true;
            }
        }
        return false;
    }
}
