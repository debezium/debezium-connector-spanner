/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.slf4j.Logger;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.task.operation.ChildPartitionOperation;
import io.debezium.connector.spanner.task.operation.ClearSharedPartitionOperation;
import io.debezium.connector.spanner.task.operation.ConnectorEndDetectionOperation;
import io.debezium.connector.spanner.task.operation.FindPartitionForStreamingOperation;
import io.debezium.connector.spanner.task.operation.Operation;
import io.debezium.connector.spanner.task.operation.PartitionStatusUpdateOperation;
import io.debezium.connector.spanner.task.operation.RemoveFinishedPartitionOperation;
import io.debezium.connector.spanner.task.operation.TakePartitionForStreamingOperation;
import io.debezium.connector.spanner.task.operation.TakeSharedPartitionOperation;
import io.debezium.connector.spanner.task.state.NewPartitionsEvent;
import io.debezium.connector.spanner.task.state.PartitionStatusUpdateEvent;
import io.debezium.connector.spanner.task.state.SyncEvent;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;

/**
 * This class processes all types of TaskStateChangeEvents (i.e. LastCommitTimestampUpdateEvent,
 * NewPartitionsEvent, NewSchemaEvent, PartitionStatusUpdateEvent, SyncEvent, TaskStateChangeEvent).
 * This class is also responsible for sending change stream partitions that are ready to be
 * streamed to SynchronizedPartitionManager.
 */
public class TaskStateChangeEventHandler {

    private static final Logger LOGGER = getLogger(TaskStateChangeEventHandler.class);

    private final TaskSyncContextHolder taskSyncContextHolder;

    private final TaskSyncPublisher taskSyncPublisher;

    private final ChangeStream changeStream;
    private final PartitionFactory partitionFactory;

    private final Runnable finishingHandler;
    private final SpannerConnectorConfig connectorConfig;
    private final SpannerEventDispatcher spannerEventDispatcher;
    private final Consumer<RuntimeException> errorHandler;

    private final AtomicLong failOverloadedTaskTimer = new AtomicLong(System.currentTimeMillis());

    public TaskStateChangeEventHandler(TaskSyncContextHolder taskSyncContextHolder,
                                       TaskSyncPublisher taskSyncPublisher,
                                       ChangeStream changeStream,
                                       PartitionFactory partitionFactory,
                                       SpannerEventDispatcher spannerEventDispatcher,
                                       Runnable finishingHandler,
                                       SpannerConnectorConfig connectorConfig,
                                       Consumer<RuntimeException> errorHandler) {
        this.taskSyncContextHolder = taskSyncContextHolder;
        this.taskSyncPublisher = taskSyncPublisher;
        this.partitionFactory = partitionFactory;
        this.changeStream = changeStream;
        this.finishingHandler = finishingHandler;
        this.connectorConfig = connectorConfig;
        this.errorHandler = errorHandler;
        this.spannerEventDispatcher = spannerEventDispatcher;
    }

    public void processEvent(TaskStateChangeEvent syncEvent) throws InterruptedException {
        LOGGER.debug("process TaskStateChangeEvent of type: {}", syncEvent.getClass().getSimpleName());

        if (syncEvent instanceof PartitionStatusUpdateEvent) {
            processEvent((PartitionStatusUpdateEvent) syncEvent);
        }
        else if (syncEvent instanceof NewPartitionsEvent) {
            processEvent((NewPartitionsEvent) syncEvent);
        }
        else if (syncEvent instanceof SyncEvent) {
            processSyncEvent();

        }
        else {
            throw new IllegalStateException("Unknown event");
        }
    }

    private void processEvent(PartitionStatusUpdateEvent event) throws InterruptedException {
        performOperation(
                new PartitionStatusUpdateOperation(event.getToken(), event.getState()),
                new FindPartitionForStreamingOperation(),
                new TakePartitionForStreamingOperation(changeStream, partitionFactory),
                new RemoveFinishedPartitionOperation(spannerEventDispatcher, connectorConfig));
    }

    private void processEvent(NewPartitionsEvent newPartitionsEvent) throws InterruptedException {
        performOperation(
                new ChildPartitionOperation(newPartitionsEvent.getPartitions()),
                new FindPartitionForStreamingOperation(),
                new TakePartitionForStreamingOperation(changeStream, partitionFactory),
                new RemoveFinishedPartitionOperation(spannerEventDispatcher, connectorConfig));
    }

    private void processSyncEvent() throws InterruptedException {
        TaskSyncContext taskSyncContext = performOperation(
                new ClearSharedPartitionOperation(),
                new TakeSharedPartitionOperation(),
                new FindPartitionForStreamingOperation(),
                new TakePartitionForStreamingOperation(changeStream, partitionFactory),
                new RemoveFinishedPartitionOperation(spannerEventDispatcher, connectorConfig),
                new ConnectorEndDetectionOperation(finishingHandler, connectorConfig.endTime()));

        failOverloadedTaskByTimer(taskSyncContext);
    }

    private void failOverloadedTaskByTimer(TaskSyncContext taskSyncContext) {
        if (!connectorConfig.failOverloadedTask()) {
            return;
        }
        synchronized (this) {
            this.failOverloadedTaskTimer.getAndUpdate(start -> {
                long now = System.currentTimeMillis();

                if (start + connectorConfig.failOverloadedTaskInterval() < now) {
                    checkToFailOverloadedTask(taskSyncContext);
                    return now;
                }

                return start;
            });
        }
    }

    private synchronized void checkToFailOverloadedTask(TaskSyncContext taskSyncContext) {
        long currentTaskPartitions = TaskStateUtil.numOwnedAndAssignedPartitions(taskSyncContext);
        long totalPartitions = TaskStateUtil.totalInProgressPartitions(taskSyncContext);

        if (currentTaskPartitions > connectorConfig.getDesiredPartitionsTasks()
                && currentTaskPartitions > 2 * (totalPartitions / (taskSyncContext.getTaskStates().size() + 1))) {
            errorHandler.accept(new SpannerConnectorException(
                    String.format("Task is overloaded by assignments: %d of total: %d", currentTaskPartitions, totalPartitions)));
        }
    }

    private TaskSyncContext performOperation(Operation... operations) throws InterruptedException {
        AtomicBoolean publishTaskSyncEvent = new AtomicBoolean(false);

        taskSyncContextHolder.lock();

        TaskSyncContext taskSyncContext;

        List<String> ownedPartitions = new ArrayList<String>();
        List<String> sharedPartitions = new ArrayList<String>();
        List<String> removedOwnedPartitions = new ArrayList<String>();
        List<String> removedSharedPartitions = new ArrayList<String>();

        long totalPartitions = taskSyncContextHolder.get().getNumPartitions() + taskSyncContextHolder.get().getNumSharedPartitions();

        try {
            taskSyncContext = taskSyncContextHolder.updateAndGet(context -> {
                TaskSyncContext newContext = context;
                for (Operation operation : operations) {
                    newContext = operation.doOperation(newContext);
                    if (operation.isRequiredPublishSyncEvent()) {
                        LOGGER.debug("Task {} - need to publish sync event for operation {}",
                                taskSyncContextHolder.get().getTaskUid(), operation.getClass().getSimpleName());
                        publishTaskSyncEvent.set(true);
                    }
                    if (!operation.updatedOwnedPartitions().isEmpty()) {
                        for (String updatedOwnedPartition : operation.updatedOwnedPartitions()) {
                            if (!ownedPartitions.contains(updatedOwnedPartition)) {
                                ownedPartitions.add(updatedOwnedPartition);
                            }
                        }
                    }

                    if (!operation.removedOwnedPartitions().isEmpty()) {
                        for (String removedOwnedPartition : operation.removedOwnedPartitions()) {
                            if (!removedOwnedPartitions.contains(removedOwnedPartition)) {
                                removedOwnedPartitions.add(removedOwnedPartition);
                            }
                        }
                    }

                    if (!operation.updatedSharedPartitions().isEmpty()) {
                        for (String updatedSharedPartition : operation.updatedSharedPartitions()) {
                            if (!sharedPartitions.contains(updatedSharedPartition)) {
                                sharedPartitions.add(updatedSharedPartition);
                            }
                        }
                    }

                    if (!operation.removedSharedPartitions().isEmpty()) {
                        for (String removedSharedPartition : operation.removedSharedPartitions()) {
                            if (!removedSharedPartitions.contains(removedSharedPartition)) {
                                removedSharedPartitions.add(removedSharedPartition);
                            }
                        }
                    }
                }
                return newContext;
            });
        }
        finally {
            taskSyncContextHolder.unlock();
        }

        if (publishTaskSyncEvent.get()) {
            TaskSyncEvent incrementalEvent = taskSyncContext.buildIncrementalTaskSyncEvent(
                    ownedPartitions, sharedPartitions, removedOwnedPartitions, removedSharedPartitions);

            long newTotalPartitions = taskSyncContext.getNumPartitions() + taskSyncContext.getNumSharedPartitions();

            if (newTotalPartitions != totalPartitions) {
                LOGGER.info(
                        "generated incremental event: Task {}  rebalance generation ID {}, task has total partitions {}, num partitions {} num shared partitions {}, incremental event {}, previous partitions {}, new partitions {}",
                        taskSyncContextHolder.get().getTaskUid(), taskSyncContextHolder.get().getRebalanceGenerationId(),
                        taskSyncContextHolder.get().getNumPartitions() + taskSyncContextHolder.get().getNumSharedPartitions(),
                        taskSyncContextHolder.get().getNumPartitions(),
                        taskSyncContextHolder.get().getNumSharedPartitions(), incrementalEvent, totalPartitions, newTotalPartitions);
            }

            taskSyncPublisher.send(incrementalEvent);
        }

        return taskSyncContext;
    }

}
