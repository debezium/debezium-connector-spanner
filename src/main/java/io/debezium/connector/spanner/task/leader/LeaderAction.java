/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader;

import static io.debezium.connector.spanner.task.LoggerUtils.debug;
import static io.debezium.connector.spanner.task.TaskStateUtil.filterSurvivedTasksStates;
import static io.debezium.connector.spanner.task.TaskStateUtil.splitSurvivedAndObsoleteTaskStates;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.kafka.internal.KafkaConsumerAdminService;
import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;
import io.debezium.connector.spanner.task.leader.rebalancer.TaskPartitionRebalancer;

/**
 * This class contains all the logic for the leader task functionality.
 *
 * Upon startup, the leader first checks if the sync topic is empty. If so, it assumes the
 * connector is operating from a fresh start and sends an initial change stream query to receive
 * back a list of initial change stream tokens.
 * Then, it waits for all survived tasks to send a list of REBALANCE_ANSWER messages.
 * Then, it rebalances change stream partitions from obsolete to new (survived) tasks.
 * Finally, it generates a new epoch message with the new assignment of partitions and writes
 * it into the sync topic.
 */

public class LeaderAction {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderAction.class);

    private static final Duration EPOCH_OFFSET_UPDATE_DURATION = Duration.ofSeconds(60);

    private final TaskSyncContextHolder taskSyncContextHolder;
    private final KafkaConsumerAdminService kafkaAdminService;

    private final LeaderService leaderService;
    private final TaskPartitionRebalancer taskPartitonRebalancer;

    private final TaskSyncPublisher taskSyncPublisher;

    private volatile Thread leaderThread;

    private Consumer<Throwable> errorHandler;

    public LeaderAction(TaskSyncContextHolder taskSyncContextHolder, KafkaConsumerAdminService kafkaAdminService,
                        LeaderService leaderService, TaskPartitionRebalancer taskPartitonRebalancer,
                        TaskSyncPublisher taskSyncPublisher,
                        Consumer<Throwable> errorHandler) {
        this.taskSyncContextHolder = taskSyncContextHolder;
        this.kafkaAdminService = kafkaAdminService;
        this.leaderService = leaderService;
        this.taskPartitonRebalancer = taskPartitonRebalancer;
        this.taskSyncPublisher = taskSyncPublisher;
        this.errorHandler = errorHandler;
    }

    private Thread createLeaderThread() {
        Thread thread = new Thread(() -> {
            LOGGER.info("performLeaderAction: Task {} start leader thread", taskSyncContextHolder.get().getTaskUid());
            try {
                this.newEpoch();
            }
            catch (InterruptedException e) {

                LOGGER.info("performLeaderAction: Task {} stop leader thread", taskSyncContextHolder.get().getTaskUid());

                Thread.currentThread().interrupt();
                return;
            }

            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(EPOCH_OFFSET_UPDATE_DURATION.toMillis());
                    if (taskSyncContextHolder.get().getRebalanceState() == RebalanceState.NEW_EPOCH_STARTED) {
                        this.publishEpochOffset();
                    }
                }
                catch (InterruptedException e) {

                    LOGGER.info("performLeaderAction: Task {} stop leader thread", taskSyncContextHolder.get().getTaskUid());

                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "SpannerConnector-LeaderAction");

        thread.setUncaughtExceptionHandler((t, ex) -> {
            LOGGER.error("Leader action execution error", ex);
            errorHandler.accept(ex);
        });

        return thread;
    }

    private TaskSyncContext publishEpochOffset() throws InterruptedException {

        TaskSyncContext taskSyncContext = taskSyncContextHolder.updateAndGet(oldContext -> oldContext.toBuilder()
                .epochOffsetHolder(oldContext.getEpochOffsetHolder().nextOffset(oldContext.getCurrentKafkaRecordOffset()))
                .build());

        taskSyncPublisher.send(taskSyncContext.buildUpdateEpochTaskSyncEvent());

        LOGGER.info("Task {} - Epoch offset has been incremented and published {}:{}",
                taskSyncContextHolder.get().getTaskUid(), taskSyncContext.getRebalanceGenerationId(), taskSyncContext.getEpochOffsetHolder().getEpochOffset());

        return taskSyncContext;
    }

    public void start() {
        if (this.leaderThread != null) {
            this.stop();
        }
        this.leaderThread = createLeaderThread();
        this.leaderThread.start();
    }

    public void stop() {
        if (this.leaderThread == null) {
            return;
        }

        this.leaderThread.interrupt();

        while (!this.leaderThread.getState().equals(Thread.State.TERMINATED)) {
        }

        this.leaderThread = null;
    }

    private void newEpoch() throws InterruptedException {
        LOGGER.info("performLeaderActions: new epoch initialization");
        boolean startFromScratch = leaderService.isStartFromScratch();

        Set<String> activeConsumers = kafkaAdminService.getActiveConsumerGroupMembers();

        LOGGER.info("performLeaderActions: consumers found {}", activeConsumers);

        Map<String, String> consumerToTaskMap = leaderService.awaitAllNewTaskStateUpdates(
                activeConsumers,
                taskSyncContextHolder.get().getRebalanceGenerationId());

        LOGGER.info("performLeaderActions: answers received {}", consumerToTaskMap);

        TaskSyncContext taskSyncContext = taskSyncContextHolder.updateAndGet(oldContext -> {
            TaskState leaderState = oldContext.getCurrentTaskState();

            Map<String, TaskState> currentTaskStates = oldContext.getAllTaskStates();

            Map<Boolean, Map<String, TaskState>> isSurvivedPartitionedTaskStates = splitSurvivedAndObsoleteTaskStates(currentTaskStates, consumerToTaskMap.values());

            Map<String, TaskState> survivedTasks = isSurvivedPartitionedTaskStates.get(true);
            Map<String, TaskState> obsoleteTasks = isSurvivedPartitionedTaskStates.get(false);

            leaderState = taskPartitonRebalancer.rebalance(leaderState, survivedTasks, obsoleteTasks);

            return oldContext.toBuilder()
                    .currentTaskState(leaderState)
                    .rebalanceState(RebalanceState.NEW_EPOCH_STARTED)
                    .taskStates(filterSurvivedTasksStates(oldContext.getTaskStates(), survivedTasks.keySet()))
                    .epochOffsetHolder(oldContext.getEpochOffsetHolder().nextOffset(oldContext.getCurrentKafkaRecordOffset()))
                    .build();
        });

        TaskSyncEvent taskSyncEvent = taskSyncContext.buildNewEpochTaskSyncEvent();

        LOGGER.info("Task {} - LeaderAction sent sync event start new epoch {}:{}", taskSyncContext.getTaskUid(),
                taskSyncContext.getRebalanceGenerationId(), taskSyncContext.getEpochOffsetHolder().getEpochOffset());

        taskSyncPublisher.send(taskSyncEvent);

        if (startFromScratch) {
            leaderService.newParentPartition();
            LOGGER.info("performLeaderActions: newParentPartition");
        }

        debug(LOGGER, "performLeaderActions: new epoch {}", taskSyncEvent);
    }

}
