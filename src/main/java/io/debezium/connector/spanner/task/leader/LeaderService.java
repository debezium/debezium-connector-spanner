/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.RebalanceMetricEvent;
import io.debezium.connector.spanner.task.PartitionFactory;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;
import io.debezium.connector.spanner.task.state.NewPartitionsEvent;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.connector.spanner.task.utils.TimeoutMeter;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.ErrorHandler;

/**
 * Provides a Leader Task functionality, after the rebalance event happens.
 *
 * Leader waits all the task answer to rebalance event, distributes partitions across
 * tasks, generates New Epoch message.
 */
public class LeaderService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderService.class);

    private static final int POLL_INTERVAL_MILLIS = 20;
    private static final Duration AWAIT_TASK_ANSWER_DURATION = Duration.of(60, ChronoUnit.SECONDS);

    private final TaskSyncContextHolder taskSyncContextHolder;

    private final Timestamp startTime;
    private final Timestamp endTime;

    private final BlockingConsumer<TaskStateChangeEvent> eventConsumer;
    private final ErrorHandler errorHandler;

    private final MetricsEventPublisher metricsEventPublisher;

    private final PartitionFactory partitionFactory;

    public LeaderService(TaskSyncContextHolder taskSyncContextHolder,
                         SpannerConnectorConfig spannerConnectorConfig,
                         BlockingConsumer<TaskStateChangeEvent> eventConsumer,
                         ErrorHandler errorHandler,
                         PartitionFactory partitionFactory,
                         MetricsEventPublisher metricsEventPublisher) {
        this.taskSyncContextHolder = taskSyncContextHolder;

        this.startTime = spannerConnectorConfig.startTime();
        this.endTime = spannerConnectorConfig.endTime() != null ? spannerConnectorConfig.endTime() : null;

        this.eventConsumer = eventConsumer;
        this.errorHandler = errorHandler;

        this.partitionFactory = partitionFactory;

        this.metricsEventPublisher = metricsEventPublisher;
    }

    public boolean isStartFromScratch() {

        EnumSet<PartitionStateEnum> inProgressStates = EnumSet.of(
                PartitionStateEnum.CREATED,
                PartitionStateEnum.READY_FOR_STREAMING,
                PartitionStateEnum.SCHEDULED,
                PartitionStateEnum.RUNNING);

        Map<String, TaskState> allTaskStates = taskSyncContextHolder.get().getAllTaskStates();

        boolean anyInProgress = allTaskStates
                .values().stream()
                .flatMap(t -> Stream.of(t.getPartitions(), t.getSharedPartitions()).flatMap(Collection::stream))
                .anyMatch(p -> inProgressStates.contains(p.getState()));

        return !anyInProgress;
    }

    public Map<String, String> awaitAllNewTaskStateUpdates(Set<String> consumers, long rebalanceGenerationId)
            throws InterruptedException {
        Map<String, String> consumerToTaskMap = new HashMap<>();
        LOGGER.info("awaitAllNewTaskStateUpdates: wait taskSyncContextHolder for all new task updates");

        TimeoutMeter timeoutMeter = TimeoutMeter.setTimeout(AWAIT_TASK_ANSWER_DURATION);

        while (consumerToTaskMap.size() < consumers.size()) {

            LOGGER.info("awaitAllNewTaskStateUpdates: " +
                    "expected: {}, actual: {}. Expected consumers: {}", consumers.size(), consumerToTaskMap.size(), consumers);

            if (timeoutMeter.isExpired()) {
                LOGGER.error("Task {} : Not received all answers from tasks", taskSyncContextHolder.get().getTaskUid());
                break;
            }

            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            }
            catch (InterruptedException e) {
                throw e; // will be handled by LeaderAction
            }

            taskSyncContextHolder.get().getAllTaskStates()
                    .entrySet()
                    .stream()
                    .filter(e -> !consumerToTaskMap.containsValue(e.getKey())
                            && consumers.contains(e.getValue().getConsumerId())
                            && e.getValue().getRebalanceGenerationId() == rebalanceGenerationId)
                    .findAny()
                    .ifPresent(state -> consumerToTaskMap.put(state.getValue().getConsumerId(), state.getKey()));

            metricsEventPublisher.publishMetricEvent(
                    new RebalanceMetricEvent(consumerToTaskMap.size(), consumers.size()));

        }
        LOGGER.info("awaitAllNewTaskStateUpdates: new task updated the state with {} consumers: {}", consumerToTaskMap, consumerToTaskMap.size());
        return consumerToTaskMap;
    }

    public void newParentPartition() throws InterruptedException {

        Partition partition = partitionFactory.initPartition(startTime, endTime);

        LOGGER.info("New parent partition {}", partition);

        eventConsumer.accept(new NewPartitionsEvent(List.of(partition)));
    }
}
