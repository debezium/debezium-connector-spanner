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
            if (metadata.isCanInitiateRebalancing()) {
                LOGGER.info("task {}, skipping stale message, finished processing all previous sync event messages with end offset {}, can initiate rebalancing",
                        taskSyncContextHolder.get().getTaskUid(), metadata.getOffset());
                taskSyncContextHolder.update(context -> context.toBuilder()
                        .rebalanceState(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)
                        .epochOffsetHolder(context.getEpochOffsetHolder().nextOffset(context.getCurrentKafkaRecordOffset()))
                        .build());
            }
            return;
        }

        if (inSync != null) {
            if (inSync.getMessageType() == MessageTypeEnum.NEW_EPOCH) {
                LOGGER.info("Task {} - processPreviousStates - merge new epoch with rebalance generation ID {}", taskSyncContextHolder.get().getTaskUid(),
                        inSync.getRebalanceGenerationId());
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

            LOGGER.info("task {}, finished processing all previous sync event messages with end offset {}, can initiate rebalancing",
                    taskSyncContextHolder.get().getTaskUid(), metadata.getOffset());

            taskSyncContextHolder.update(context -> context.toBuilder()
                    .rebalanceState(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)
                    .epochOffsetHolder(context.getEpochOffsetHolder().nextOffset(context.getCurrentKafkaRecordOffset()))
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

            if (!taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.NEW_EPOCH_STARTED)) {
                return;
            }

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

            if (!taskSyncContextHolder.get().isLeader() ||
                    !taskSyncContextHolder.get().getRebalanceState().equals(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)) {
                return;
            }

            LOGGER.info("Task {} - process sync event - rebalance answer", taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> SyncEventMerger.mergeRebalanceAnswer(context, inSync));

        }
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    public void process(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException, IllegalStateException {
        if (inSync == null) {
            return;
        }

        if (skipFromPreviousGeneration(inSync)) {
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
            LOGGER.error("Exception during processing task message {}, {}", inSync, e);
            throw e;
        }
    }

    private boolean skipFromPreviousGeneration(TaskSyncEvent inSync) {
        if (inSync != null) {
            long inGeneration = inSync.getRebalanceGenerationId();
            long currentGeneration = taskSyncContextHolder.get().getRebalanceGenerationId();

            if (inGeneration < currentGeneration) {
                LOGGER.info("skipFromPreviousGeneration: currentGen: {}, inGen: {}, inTaskUid: {}", currentGeneration, inGeneration, inSync.getTaskUid());
                return true;
            }
        }
        return false;
    }
}
