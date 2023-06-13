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
            return;
        }

        taskSyncContextHolder.lock();
        try {

            if (inSync != null) {
                if (inSync.getMessageType() == MessageTypeEnum.NEW_EPOCH) {
                    LOGGER.info("Task {} - processPreviousStates - merge new epoch with rebalance generation ID {}", taskSyncContextHolder.get().getTaskUid(),
                            inSync.getRebalanceGenerationId());
                    taskSyncContextHolder.update(context -> SyncEventMerger.mergeNewEpoch(context, inSync));
                }
                else {
                    taskSyncContextHolder.update(context -> SyncEventMerger.merge(context, inSync));
                }
            }

            if (metadata.isCanInitiateRebalancing()) {
                LOGGER.info("task {}, finished processing all previous sync event messages with end offset {}, can initiate rebalancing", taskSyncContextHolder.get().getTaskUid(), endOffset);

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
        finally {
            taskSyncContextHolder.unlock();
        }
    }

    public void processNewEpoch(TaskSyncEvent inSync, SyncEventMetadata metadata) throws InterruptedException, IllegalStateException {
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

                LOGGER.debug("Task {} - processNewEpoch {}", taskSyncContextHolder.get().getTaskUid(), inSync);
                LOGGER.info("Task {} - processNewEpoch : metadata {}, rebalanceId: {}",
                        taskSyncContextHolder.get().getTaskUid(),
                        metadata,
                        taskSyncContextHolder.get().getRebalanceGenerationId());

                taskSyncContextHolder.update(context -> SyncEventMerger.mergeNewEpoch(context, inSync));
                LOGGER.info("Task {} - SyncEventHandler sent response for new epoch", taskSyncContextHolder.get().getTaskUid());

                taskSyncPublisher.send(taskSyncContextHolder.get().buildTaskSyncEvent());
            }
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

            LOGGER.info("Task {} - process sync event - rebalance answer", taskSyncContextHolder.get().getTaskUid());

            taskSyncContextHolder.update(context -> SyncEventMerger.merge(context, inSync));

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
