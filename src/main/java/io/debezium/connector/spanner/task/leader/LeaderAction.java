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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.spanner.kafka.internal.KafkaConsumerAdminService;
import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;
import io.debezium.connector.spanner.task.leader.rebalancer.TaskPartitionRebalancer;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

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

    private static final Duration EPOCH_OFFSET_UPDATE_DURATION = Duration.ofSeconds(300);

    private final TaskSyncContextHolder taskSyncContextHolder;
    private final KafkaConsumerAdminService kafkaAdminService;

    private final LeaderService leaderService;
    private final TaskPartitionRebalancer taskPartitonRebalancer;

    private final TaskSyncPublisher taskSyncPublisher;

    private volatile Thread leaderThread;

    private Consumer<Throwable> errorHandler;

    private final Duration sleepInterval = Duration.ofMillis(100);

    private final Clock clock;

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
        this.clock = Clock.system();
    }

    private Thread createLeaderThread(long rebalanceGenerationId) {
        Thread thread = new Thread(() -> {
            LOGGER.info("performLeaderAction: Task {} start leader thread with rebalance generation ID {}", taskSyncContextHolder.get().getTaskUid(),
                    rebalanceGenerationId);
            try {
                this.newEpoch(rebalanceGenerationId);
            }
            catch (InterruptedException e) {

                LOGGER.info("performLeaderAction: Task {} stop leader thread", taskSyncContextHolder.get().getTaskUid());

                Thread.currentThread().interrupt();
                return;
            }

            while (!Thread.currentThread().isInterrupted()) {
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
            LOGGER.info("performLeaderAction: Task {} stopped leader thread", taskSyncContextHolder.get().getTaskUid());
        }, "SpannerConnector-LeaderAction");

        thread.setUncaughtExceptionHandler((t, ex) -> {
            LOGGER.error("Leader action execution error, task {}, ex", taskSyncContextHolder.get().getTaskUid(), ex);
            errorHandler.accept(ex);
        });

        return thread;
    }

    private TaskSyncContext publishEpochOffset() throws InterruptedException {

        TaskSyncContext taskSyncContext = taskSyncContextHolder.updateAndGet(oldContext -> oldContext.toBuilder()
                .epochOffsetHolder(oldContext.getEpochOffsetHolder().nextOffset(oldContext.getCurrentKafkaRecordOffset()))
                .build());

        TaskSyncEvent taskSyncEvent = taskSyncContext.buildUpdateEpochTaskSyncEvent();
        taskSyncPublisher.send(taskSyncEvent);

        int numPartitions = taskSyncEvent.getNumPartitions();

        int numSharedPartitions = taskSyncEvent.getNumSharedPartitions();

        LOGGER.info(
                "Task {} - Leader task has updated the epoch offset with rebalance generation ID: {} and epoch offset: {}, num partitions {}, num shared partitions {}",
                taskSyncContextHolder.get().getTaskUid(), taskSyncContext.getRebalanceGenerationId(), taskSyncContext.getEpochOffsetHolder().getEpochOffset(),
                numPartitions, numSharedPartitions);

        return taskSyncContext;
    }

    public void start(long rebalanceGenerationId) {
        if (this.leaderThread != null) {
            this.stop(rebalanceGenerationId);
        }
        this.leaderThread = createLeaderThread(rebalanceGenerationId);
        this.leaderThread.start();
    }

    public void stop(long rebalanceGenerationId) {
        if (this.leaderThread == null) {
            return;
        }
        LOGGER.info("Task {}, trying to stop leader thread with rebalance generation ID {}", taskSyncContextHolder.get().getTaskUid(),
                rebalanceGenerationId);

        this.leaderThread.interrupt();
        LOGGER.info("Task {}, interrupted leader thread with rebalance generation ID {}", taskSyncContextHolder.get().getTaskUid(),
                rebalanceGenerationId);

        final Metronome metronome = Metronome.sleeper(sleepInterval, clock);
        while (!this.leaderThread.getState().equals(Thread.State.TERMINATED)) {
            try {
                // Sleep for sleepInterval.
                metronome.pause();

                this.leaderThread.interrupt();

                LOGGER.info("Task {}, still waiting for leader thread to die with rebalance generation ID {}", taskSyncContextHolder.get().getTaskUid(),
                        rebalanceGenerationId);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info("Task {}, stopped leader thread with rebalance generation ID {}", taskSyncContextHolder.get().getTaskUid(),
                rebalanceGenerationId);

        this.leaderThread = null;
    }

    private void newEpoch(long rebalanceGenerationId) throws InterruptedException {
        LOGGER.info("performLeaderActions: new epoch initialization");
        boolean startFromScratch = leaderService.isStartFromScratch();

        Set<String> activeConsumers = kafkaAdminService.getActiveConsumerGroupMembers();

        LOGGER.info("Task {} with consumer {}, performLeaderActions: consumers found {}", taskSyncContextHolder.get().getTaskUid(),
                taskSyncContextHolder.get().getConsumerId(), activeConsumers);

        activeConsumers.remove(taskSyncContextHolder.get().getConsumerId());

        LOGGER.info("Task {} with consumer {}, performLeaderActions: consumers found without leader {}", taskSyncContextHolder.get().getTaskUid(),
                taskSyncContextHolder.get().getConsumerId(), activeConsumers);

        Map<String, String> consumerToTaskMap = leaderService.awaitAllNewTaskStateUpdates(
                activeConsumers,
                rebalanceGenerationId);

        LOGGER.info("performLeaderActions: answers received {}", consumerToTaskMap);

        if (consumerToTaskMap.size() < activeConsumers.size()) {
            LOGGER.info("TaskUid {}, Expected active consumers {}, but only received consumers {}, not sending new epoch", taskSyncContextHolder.get().getTaskUid(),
                    activeConsumers, consumerToTaskMap);
            throw new DebeziumException("Task Uid " + taskSyncContextHolder.get().getTaskUid() + " Expected active consumers " + activeConsumers.toString()
                    + " but only received consumers " + consumerToTaskMap.toString() + " not sending new epoch ");
        }

        // Check if there is any duplicate partitions in the task sync context before rebalancing.
        TaskSyncContext staleContext = taskSyncContextHolder.get();
        boolean foundDuplication = false;
        if (staleContext.checkDuplication(false, "NEW EPOCH rebalance event, initial context")) {
            foundDuplication = true;

        }
        int numOldPartitions = taskSyncContextHolder.get().getNumPartitions();
        int numSharedOldPartitions = taskSyncContextHolder.get().getNumSharedPartitions();

        LOGGER.info("Task {} - before sending new epoch, total partitions {}, num partitions {}, num shared partitions {}", numOldPartitions + numSharedOldPartitions,
                numOldPartitions, numSharedOldPartitions);
        TaskSyncContext taskSyncContext = taskSyncContextHolder.updateAndGet(oldContext -> {
            TaskState leaderState = oldContext.getCurrentTaskState();

            // Only get the nonleader current task states to split between survived and obsolete.
            Map<String, TaskState> currentTaskStates = oldContext.getTaskStates();
            Set<String> taskUids = currentTaskStates.keySet().stream().collect(Collectors.toSet());
            LOGGER.info("Task {}, Current task states in old context: {}", oldContext.getTaskUid(), taskUids);

            Map<Boolean, Map<String, TaskState>> isSurvivedPartitionedTaskStates = splitSurvivedAndObsoleteTaskStates(currentTaskStates, consumerToTaskMap.values());

            Map<String, TaskState> survivedTasks = isSurvivedPartitionedTaskStates.get(true);
            Map<String, TaskState> obsoleteTasks = isSurvivedPartitionedTaskStates.get(false);

            // Update the current task's rebalance generation ID when sending out the new epoch.
            leaderState = taskPartitonRebalancer.rebalance(leaderState, survivedTasks, obsoleteTasks);
            TaskState finalLeaderState = leaderState.toBuilder().rebalanceGenerationId(rebalanceGenerationId).build();

            return oldContext.toBuilder()
                    .currentTaskState(finalLeaderState)
                    .rebalanceState(RebalanceState.NEW_EPOCH_STARTED)
                    .rebalanceGenerationId(rebalanceGenerationId)
                    .taskStates(filterSurvivedTasksStates(oldContext.getTaskStates(), survivedTasks.keySet()))
                    .epochOffsetHolder(oldContext.getEpochOffsetHolder().nextOffset(oldContext.getCurrentKafkaRecordOffset()))
                    .build();
        });

        if (!foundDuplication) {
            taskSyncContext.checkDuplication(true, "NEW EPOCH rebalance event, resulting context");
        }

        TaskSyncEvent taskSyncEvent = taskSyncContext.buildNewEpochTaskSyncEvent();

        int numPartitions = taskSyncEvent.getNumPartitions();

        int numSharedPartitions = taskSyncEvent.getNumSharedPartitions();

        Set<String> taskUids = taskSyncEvent.getTaskStates().keySet().stream().collect(Collectors.toSet());

        LOGGER.info(
                "Task {} - sent new epoch with rebalance generation ID {}, num tasks {}, total partitions {}, num owned partitions {}, num shared partitions {}, task Uids {}",
                taskSyncContext.getTaskUid(),
                taskSyncContext.getRebalanceGenerationId(),
                taskSyncEvent.getTaskStates().size(),
                numPartitions + numSharedPartitions,
                numPartitions, numSharedPartitions, taskUids, numPartitions + numSharedPartitions);

        taskSyncPublisher.send(taskSyncEvent);

        if (startFromScratch) {
            leaderService.newParentPartition();
            LOGGER.info("performLeaderActions: newParentPartition");
        }

        debug(LOGGER, "performLeaderActions: new epoch {}", taskSyncEvent);
    }

}
