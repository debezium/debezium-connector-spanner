/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.SyncEventMetadata;
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
        if (skipFromMismatchingGeneration(inSync)) {
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
        if (skipFromMismatchingGeneration(inSync)) {
            if (inSync != null) {
                long inGeneration = inSync.getRebalanceGenerationId();
                long currentGeneration = taskSyncContextHolder.get().getRebalanceGenerationId();
                LOGGER.debug("Task {}, skipFromMismatchingGeneration: currentGen: {}, inGen: {}, inTaskUid: {}, message type {}", taskSyncContextHolder.get().getTaskUid(),
                        currentGeneration, inGeneration,
                        inSync.getTaskUid(),
                        inSync.getMessageType());
            }
            if (metadata.isCanInitiateRebalancing()) {
                LOGGER.info("task {}, finished processing all previous sync event messages with end offset {}, can initiate rebalancing with rebalance generation ID {}",
                        taskSyncContextHolder.get().getTaskUid(), metadata.getOffset(), taskSyncContextHolder.get().getRebalanceGenerationId());

                taskSyncContextHolder.update(context -> context.toBuilder()
                        .rebalanceState(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)
                        .build());
            }
            return;
        }

        if (inSync != null) {
            if (inSync.getMessageType() == MessageTypeEnum.NEW_EPOCH) {
                taskSyncContextHolder.update(context -> SyncEventMerger.mergeNewEpoch(context, inSync));
            }
            else if (inSync.getMessageType() == MessageTypeEnum.REBALANCE_ANSWER) {
                taskSyncContextHolder.update(context -> SyncEventMerger.mergeRebalanceAnswer(context, inSync));
            }
            else if (inSync.getMessageType() == MessageTypeEnum.UPDATE_EPOCH) {

                taskSyncContextHolder.update(context -> SyncEventMerger.mergeEpochUpdate(context, inSync));
            }
            else {
                taskSyncContextHolder.update(context -> SyncEventMerger.mergeIncrementalTaskSyncEvent(context, inSync));
            }
        }

        if (metadata.isCanInitiateRebalancing()) {
            LOGGER.info("Task {} - processPreviousStates - switch state to INITIAL_INCREMENTED_STATE_COMPLETED",
                    taskSyncContextHolder.get().getTaskUid());

            long newPartitions = taskSyncContextHolder.get().getNumPartitions() + taskSyncContextHolder.get().getNumSharedPartitions();

            LOGGER.info(
                    "task {}, finished processing all previous sync event messages with end offset {}, can initiate rebalancing with rebalance generation ID {}, task has total partitions {}",
                    taskSyncContextHolder.get().getTaskUid(), metadata.getOffset(), taskSyncContextHolder.get().getRebalanceGenerationId(), newPartitions);

            taskSyncContextHolder.update(context -> context.toBuilder()
                    .rebalanceState(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)
                    .build());
            LOGGER.info("Task {} - now initialized with epoch offset {} and context {}", taskSyncContextHolder.get().getTaskUid(),
                    taskSyncContextHolder.get().getEpochOffsetHolder().getEpochOffset(), taskSyncContextHolder.get());

            // Check that there are no duplicate partitions after the task has finished initializing.
            taskSyncContextHolder.get().checkDuplication(true, "Finished Initializing Task State");
        }
    }

    public void processNewEpoch(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException, IllegalStateException {
        taskSyncContextHolder.lock();
        try {

            LOGGER.info("Task {} - processNewEpoch {}: metadata {}, rebalanceId: {}, current task {}",
                    taskSyncContextHolder.get().getTaskUid(),
                    inSync,
                    taskSyncContextHolder.get(),
                    metadata,
                    inSync.getRebalanceGenerationId());

            taskSyncContextHolder.update(context -> SyncEventMerger.mergeNewEpoch(context, inSync));

            LOGGER.info("Task {} - SyncEventHandler sent response for new epoch",
                    taskSyncContextHolder.get().getTaskUid());

            taskSyncPublisher.send(taskSyncContextHolder.get().buildCurrentTaskSyncEvent());
        }
        finally {
            taskSyncContextHolder.unlock();
        }

    }

    public void processUpdateEpoch(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException {
        taskSyncContextHolder.lock();
        try {
            boolean start_initial_sync = taskSyncContextHolder.get().getRebalanceState() == RebalanceState.START_INITIAL_SYNC;

            if (!start_initial_sync && !taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.NEW_EPOCH_STARTED)) {
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

    public void processRegularMessage(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException {
        taskSyncContextHolder.lock();

        try {
            LOGGER.debug("Task {} - process sync event", taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> SyncEventMerger.mergeIncrementalTaskSyncEvent(context, inSync));

            eventConsumer.accept(new SyncEvent());

        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    public void processRebalanceAnswer(TaskSyncEvent inSync, SyncEventMetadata metadata) {
        taskSyncContextHolder.lock();

        try {

            if (!taskSyncContextHolder.get().isLeader() || !taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)) {
                return;
            }

            LOGGER.info("Task {} - process sync event - rebalance answer", taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> SyncEventMerger.mergeRebalanceAnswer(context, inSync));

        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    private boolean skipFromMismatchingGeneration(TaskSyncEvent inSync) {
        if (inSync != null) {
            long inGeneration = inSync.getRebalanceGenerationId();
            long currentGeneration = taskSyncContextHolder.get().getRebalanceGenerationId();

            // For REGULAR type messages, we filter them out in SyncEventMerger if the preexisting
            // task states map does not contain them, and if the state timestamp is not greater.

            if (inSync.getMessageType() == MessageTypeEnum.REBALANCE_ANSWER ||
                    inSync.getMessageType() == MessageTypeEnum.NEW_EPOCH
                    || inSync.getMessageType() == MessageTypeEnum.UPDATE_EPOCH) {
                return inGeneration < currentGeneration;
            }
        }
        return false;
    }

    public void process(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException, IllegalStateException {
        if (inSync == null) {
            return;
        }

        if (skipFromMismatchingGeneration(inSync)) {
            LOGGER.debug("Task {}, skipping message from task {}, from prior generation {} and message type {} with current generation {}",
                    taskSyncContextHolder.get().getTaskUid(), inSync.getTaskUid(), inSync.getRebalanceGenerationId(), inSync.getMessageType(),
                    taskSyncContextHolder.get().getRebalanceGenerationId());
            return;
        }

        try {
            if (inSync.getMessageType() == MessageTypeEnum.REGULAR) {
                processRegularMessage(inSync, metadata);

            }
            else if (inSync.getMessageType() == MessageTypeEnum.REBALANCE_ANSWER) {
                processRebalanceAnswer(inSync, metadata);

            }
            else if (inSync.getMessageType() == MessageTypeEnum.UPDATE_EPOCH) {
                processUpdateEpoch(inSync, metadata);

            }
            else if (inSync.getMessageType() == MessageTypeEnum.NEW_EPOCH) {
                processNewEpoch(inSync, metadata);

            }
        }
        catch (Exception e) {
            LOGGER.error("Exception during processing task message task Uid {}, message type {}, {}", inSync.getTaskUid(), inSync.getMessageType(), e);
            throw e;
        }
    }
}
