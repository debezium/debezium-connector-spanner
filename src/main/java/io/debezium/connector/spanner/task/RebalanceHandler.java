/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.task.leader.LeaderAction;
import io.debezium.connector.spanner.task.leader.LowWatermarkStampPublisher;

/**
 * Provides a logic for processing Rebalance Events.
 * If current task become a leader - starts {@link LeaderAction}
 */
public class RebalanceHandler {
    private static final Logger LOGGER = getLogger(RebalanceHandler.class);

    private final TaskSyncContextHolder taskSyncContextHolder;

    private final TaskSyncPublisher taskSyncPublisher;

    private final LeaderAction leaderAction;

    private final LowWatermarkStampPublisher lowWatermarkStampPublisher;

    public RebalanceHandler(TaskSyncContextHolder taskSyncContextHolder, TaskSyncPublisher taskSyncPublisher,
                            LeaderAction leaderAction, LowWatermarkStampPublisher lowWatermarkStampPublisher) {
        this.taskSyncContextHolder = taskSyncContextHolder;
        this.taskSyncPublisher = taskSyncPublisher;
        this.leaderAction = leaderAction;
        this.lowWatermarkStampPublisher = lowWatermarkStampPublisher;
    }

    public void process(boolean isLeader, String consumerId, long rebalanceGenerationId) throws InterruptedException {
        LOGGER.info(
                "processRebalancingEvent: start, consumerId: {}, taskId{}, rebalanceGenerationId: {}, isLeader {}",
                consumerId,
                taskSyncContextHolder.get().getTaskUid(),
                rebalanceGenerationId, isLeader);

        long oldRebalanceGenerationId = taskSyncContextHolder.get().getRebalanceGenerationId();
        if (rebalanceGenerationId < oldRebalanceGenerationId) {
            LOGGER.info(
                    "processRebalancingEvent: skipping due to stale rebalance generation ID {}, consumerId: {}, taskId{}, rebalanceGenerationId: {}, isLeader {}",
                    rebalanceGenerationId, consumerId,
                    taskSyncContextHolder.get().getTaskUid(),
                    rebalanceGenerationId, isLeader);
            return;
        }

        // Do not update the rebalance generation ID here. Only update when:
        // 1. sending out the new Epoch
        // 2. processing the new epoch
        TaskSyncContext context = taskSyncContextHolder.updateAndGet(oldContext -> {
            TaskState updatedState = oldContext.getCurrentTaskState()
                    .toBuilder()
                    .consumerId(consumerId)
                    .build();

            return oldContext.toBuilder()
                    .consumerId(consumerId)
                    .currentTaskState(updatedState)
                    .isLeader(isLeader)

                    // If the task survived after a rebalance event, we need to change its
                    // rebalance state from the NEW_EPOCH_STARTED to
                    // INITIAL_INCREMENTED_STATE_COMPLETED, so that it doesn't share change
                    // stream partitions until after the leader finishes rebalancing change
                    // stream partitions from the obsolete tasks to the new survived tasks.
                    .rebalanceState(RebalanceState.INITIAL_INCREMENTED_STATE_COMPLETED)
                    .build();
        });

        LOGGER.info("Task {} - RebalanceHandler finished updating task sync context for consumer ID {} and rebalance generation ID {}", context.getTaskUid(),
                consumerId,
                rebalanceGenerationId);
        // Stop the current leader thread if it exists.
        this.leaderAction.stop(oldRebalanceGenerationId);

        LOGGER.info("processRebalancingEvent: stopped leader thread, consumerId: {}, taskId{}, rebalanceGenerationId: {}, isLeader {}", consumerId,
                taskSyncContextHolder.get().getTaskUid(),
                rebalanceGenerationId, isLeader);

        TaskSyncEvent taskSyncEvent = context.buildRebalanceAnswerTaskSyncEvent(rebalanceGenerationId);

        LoggerUtils.debug(LOGGER, "processRebalancingEvent: send: {}", taskSyncEvent);
        LOGGER.info("Task {} - RebalanceHandler sent sync event for consumer ID {} and rebalance generation ID {}", taskSyncEvent.getTaskUid(), consumerId,
                rebalanceGenerationId);

        // Send the rebalance event.
        taskSyncPublisher.send(taskSyncEvent);

        LOGGER.info(
                "processRebalancingEvent: Task {} rebalance answer has been sent for consumer ID {} and rebalance generation ID {}, num partitions {} num shared partitions {}",
                taskSyncContextHolder.get().getTaskUid(), consumerId, rebalanceGenerationId,
                taskSyncContextHolder.get().getNumPartitions(),
                taskSyncContextHolder.get().getNumSharedPartitions());
        // Start the leader thread if the task is a leader.
        if (isLeader) {
            LOGGER.info("Task {} is leader", context.getTaskUid());
            this.leaderAction.start(rebalanceGenerationId);
            this.lowWatermarkStampPublisher.start();
        }
        else {
            this.lowWatermarkStampPublisher.suspend();
        }
    }

    public void init() {
        this.lowWatermarkStampPublisher.init();
    }

    public void destroy() {
        this.leaderAction.stop(taskSyncContextHolder.get().getRebalanceGenerationId());
        LOGGER.info("Task {}, destroyed leader action ", taskSyncContextHolder.get().getTaskUid());

        try {
            this.lowWatermarkStampPublisher.destroy();
        }

        catch (InterruptedException e) {
            // ignore to continue task destroying
        }
        LOGGER.info("Task {}, destroyed low watermark stamp publisher ", taskSyncContextHolder.get().getTaskUid());

    }
}
