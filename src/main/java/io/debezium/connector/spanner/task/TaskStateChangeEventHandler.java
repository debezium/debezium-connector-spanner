/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.task.operation.CheckPartitionDuplicationOperation;
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
    private final Timestamp endTime;

    public TaskStateChangeEventHandler(TaskSyncContextHolder taskSyncContextHolder,
                                       TaskSyncPublisher taskSyncPublisher,
                                       ChangeStream changeStream,
                                       PartitionFactory partitionFactory,
                                       Runnable finishingHandler,
                                       Timestamp endTime) {
        this.taskSyncContextHolder = taskSyncContextHolder;
        this.taskSyncPublisher = taskSyncPublisher;
        this.partitionFactory = partitionFactory;
        this.changeStream = changeStream;
        this.finishingHandler = finishingHandler;
        this.endTime = endTime;
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
                new RemoveFinishedPartitionOperation());
    }

    private void processEvent(NewPartitionsEvent newPartitionsEvent) throws InterruptedException {
        performOperation(
                new ChildPartitionOperation(newPartitionsEvent.getPartitions()),
                new FindPartitionForStreamingOperation(),
                new CheckPartitionDuplicationOperation(changeStream),
                new TakePartitionForStreamingOperation(changeStream, partitionFactory),
                new RemoveFinishedPartitionOperation());
    }

    private void processSyncEvent() throws InterruptedException {
        performOperation(
                new ClearSharedPartitionOperation(),
                new TakeSharedPartitionOperation(),
                new CheckPartitionDuplicationOperation(changeStream),
                new FindPartitionForStreamingOperation(),
                new TakePartitionForStreamingOperation(changeStream, partitionFactory),
                new RemoveFinishedPartitionOperation(),
                new ConnectorEndDetectionOperation(finishingHandler, endTime));
    }

    private void performOperation(Operation... operations) throws InterruptedException {
        AtomicBoolean publishTaskSyncEvent = new AtomicBoolean(false);

        taskSyncContextHolder.lock();

        TaskSyncContext taskSyncContext;

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
                }
                return newContext;
            });
        }
        finally {
            taskSyncContextHolder.unlock();
        }

        if (publishTaskSyncEvent.get()) {
            LOGGER.debug("Task {} - send sync event", taskSyncContext.getTaskUid());
            taskSyncPublisher.send(taskSyncContext.buildTaskSyncEvent());
        }
    }

}
