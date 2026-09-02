/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.slf4j.Logger;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.task.operation.ChildPartitionOperation;
import io.debezium.connector.spanner.task.operation.ClearSharedPartitionOperation;
import io.debezium.connector.spanner.task.operation.ConnectorEndDetectionOperation;
import io.debezium.connector.spanner.task.operation.FindPartitionForStreamingOperation;
import io.debezium.connector.spanner.task.operation.MoveInStateUpdateOperation;
import io.debezium.connector.spanner.task.operation.MoveOutStateUpdateOperation;
import io.debezium.connector.spanner.task.operation.Operation;
import io.debezium.connector.spanner.task.operation.PartitionStatusUpdateOperation;
import io.debezium.connector.spanner.task.operation.PublishMoveInStateOperation;
import io.debezium.connector.spanner.task.operation.RemoveFinishedPartitionOperation;
import io.debezium.connector.spanner.task.operation.TakePartitionForStreamingOperation;
import io.debezium.connector.spanner.task.operation.TakeSharedPartitionOperation;
import io.debezium.connector.spanner.task.operation.WindowAdvancedOperation;
import io.debezium.connector.spanner.task.state.MoveInNotificationEvent;
import io.debezium.connector.spanner.task.state.MoveInPublishOnlyEvent;
import io.debezium.connector.spanner.task.state.MoveOutNotificationEvent;
import io.debezium.connector.spanner.task.state.NewPartitionsEvent;
import io.debezium.connector.spanner.task.state.PartitionStatusUpdateEvent;
import io.debezium.connector.spanner.task.state.SyncEvent;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.connector.spanner.task.state.WindowAdvancedEvent;

/**
 * This class processes all types of TaskStateChangeEvents (i.e. LastCommitTimestampUpdateEvent,
 * NewPartitionsEvent, NewSchemaEvent, PartitionStatusUpdateEvent, SyncEvent, TaskStateChangeEvent).
 * This class is also responsible for sending change stream partitions that are ready to be
 * streamed to SynchronizedPartitionManager.
 *
 * <p>The {@link io.debezium.connector.spanner.task.TaskStateChangeEventProcessor} drives this
 * handler from a single thread. Fast events (e.g. {@link WindowAdvancedEvent}) are handled
 * entirely on that thread. Heavy events that trigger
 * {@link io.debezium.connector.spanner.task.operation.TakePartitionForStreamingOperation} — which
 * performs a blocking Kafka offset lookup — are split: the fast state-machine phase runs
 * synchronously, then the blocking partition-submission phase is offloaded to a dedicated
 * single-threaded executor ({@code partitionSchedulingExecutor}). This keeps the event-processor
 * thread free so that window-advance events continue to be processed without starvation.
 *
 * <p>Thread safety: {@link TaskSyncContextHolder#updateAndGet} serialises concurrent state
 * mutations via a {@link java.util.concurrent.locks.ReentrantLock}, so both the event-processor
 * thread and the partition-scheduling thread can call {@link #performOperation} safely.
 */
public class TaskStateChangeEventHandler {

    private static final Logger LOGGER = getLogger(TaskStateChangeEventHandler.class);

    private static final AtomicInteger SCHEDULER_THREAD_COUNTER = new AtomicInteger(0);

    private final TaskSyncContextHolder taskSyncContextHolder;

    private final TaskSyncPublisher taskSyncPublisher;

    private final ChangeStream changeStream;
    private final PartitionFactory partitionFactory;

    private final Runnable finishingHandler;
    private final SpannerConnectorConfig connectorConfig;
    private final SpannerEventDispatcher spannerEventDispatcher;
    private final Consumer<RuntimeException> errorHandler;

    private final AtomicLong failOverloadedTaskTimer = new AtomicLong(System.currentTimeMillis());

    /**
     * Dedicated single-threaded executor for the blocking offset-retrieval + partition-submission
     * step. Keeping it separate from the event-processor thread ensures that
     * {@link WindowAdvancedEvent} processing (and thus low-watermark advancement) is never stalled
     * by a slow Kafka {@link org.apache.kafka.connect.storage.OffsetStorageReader} call.
     */
    private final ExecutorService partitionSchedulingExecutor;

    /**
     * Guards against queuing more than one pending scheduling task at a time. If a scheduling task
     * is already queued (but not yet running), subsequent triggers are coalesced into it: the
     * running task always reads the current state, so it will pick up any partitions that became
     * READY_FOR_STREAMING after it was enqueued.
     */
    private final AtomicBoolean schedulingPending = new AtomicBoolean(false);

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
        this.partitionSchedulingExecutor = Executors.newSingleThreadExecutor(new PartitionSchedulerThreadFactory());
    }

    /**
     * Shuts down the partition-scheduling executor. Must be called after the
     * {@link TaskStateChangeEventProcessor} has been stopped so that no new scheduling tasks can
     * be submitted after this point.
     */
    public void shutdown() {
        partitionSchedulingExecutor.shutdown();
        try {
            if (!partitionSchedulingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warn("Task {}, partition scheduling executor did not terminate cleanly, forcing shutdown",
                        taskSyncContextHolder.get().getTaskUid());
                partitionSchedulingExecutor.shutdownNow();
                if (!partitionSchedulingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.warn("Task {}, partition scheduling executor did not terminate even after forced shutdown",
                            taskSyncContextHolder.get().getTaskUid());
                }
            }
        }
        catch (InterruptedException e) {
            partitionSchedulingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class PartitionSchedulerThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "SpannerConnector-PartitionScheduler-" + SCHEDULER_THREAD_COUNTER.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }

    public void processEvent(TaskStateChangeEvent syncEvent) throws InterruptedException {
        LOGGER.debug("process TaskStateChangeEvent of type: {}", syncEvent.getClass().getSimpleName());

        long nowMillis = Instant.now().toEpochMilli();
        if (syncEvent instanceof PartitionStatusUpdateEvent) {
            processEvent((PartitionStatusUpdateEvent) syncEvent);
        }
        else if (syncEvent instanceof NewPartitionsEvent) {
            processEvent((NewPartitionsEvent) syncEvent);
        }
        else if (syncEvent instanceof SyncEvent) {
            processSyncEvent();

        }
        else if (syncEvent instanceof MoveOutNotificationEvent) {
            processEvent((MoveOutNotificationEvent) syncEvent);
        }
        else if (syncEvent instanceof MoveInNotificationEvent) {
            processEvent((MoveInNotificationEvent) syncEvent);
        }
        else if (syncEvent instanceof MoveInPublishOnlyEvent) {
            processEvent((MoveInPublishOnlyEvent) syncEvent);
        }
        else if (syncEvent instanceof WindowAdvancedEvent) {
            processEvent((WindowAdvancedEvent) syncEvent);
        }
        else {
            throw new IllegalStateException("Unknown event");
        }
        long thenMillis = Instant.now().toEpochMilli();
        LOGGER.debug(
                "Task {}, TaskStateChangeEventHandler: Processed {} in {} millis",
                taskSyncContextHolder.get().getTaskUid(), syncEvent.getClass().getSimpleName(), thenMillis - nowMillis);

    }

    private void processEvent(PartitionStatusUpdateEvent event) throws InterruptedException {
        // Fast state-machine phase: runs on the event-processor thread.
        performOperation(
                new PartitionStatusUpdateOperation(event.getToken(), event.getState()),
                new ClearSharedPartitionOperation(),
                new FindPartitionForStreamingOperation(changeStream.isMutableKeyRange()));
        // Blocking offset-fetch + stream-submission phase: offloaded to dedicated executor.
        schedulePendingPartitionsAsync();
    }

    private void processEvent(NewPartitionsEvent newPartitionsEvent) throws InterruptedException {
        // Fast state-machine phase: runs on the event-processor thread.
        performOperation(
                new ChildPartitionOperation(newPartitionsEvent.getPartitions()),
                new ClearSharedPartitionOperation(),
                new FindPartitionForStreamingOperation(changeStream.isMutableKeyRange()),
                new RemoveFinishedPartitionOperation(spannerEventDispatcher, connectorConfig));
        // Blocking offset-fetch + stream-submission phase: offloaded to dedicated executor.
        schedulePendingPartitionsAsync();
    }

    private void processEvent(MoveOutNotificationEvent event) throws InterruptedException {
        // Fast state-machine phase: runs on the event-processor thread.
        performOperation(
                new MoveOutStateUpdateOperation(
                        event.getToken(), event.getCommitTimestamp(), event.getDestinationTokens()),
                new FindPartitionForStreamingOperation(changeStream.isMutableKeyRange()));
        // Blocking offset-fetch + stream-submission phase: offloaded to dedicated executor.
        schedulePendingPartitionsAsync();
    }

    private void processEvent(MoveInNotificationEvent event) throws InterruptedException {
        // Fast state-machine phase: runs on the event-processor thread.
        performOperation(
                new MoveInStateUpdateOperation(
                        event.getToken(), event.getCommitTimestamp(), event.getRecordSequence(), event.getSourcePartitionTokens()),
                new FindPartitionForStreamingOperation(changeStream.isMutableKeyRange()));
        // Blocking offset-fetch + stream-submission phase: offloaded to dedicated executor.
        schedulePendingPartitionsAsync();
    }

    private void processEvent(MoveInPublishOnlyEvent event) throws InterruptedException {
        // Buffer-gate path: publish MoveInState to sync topic for cross-task visibility and
        // crash-recovery. The partition does NOT transition to CREATED; the streaming thread
        // stays alive and self-gates. No TakePartitionForStreamingOperation is needed.
        performOperation(
                new PublishMoveInStateOperation(
                        event.getToken(), event.getCommitTimestamp(), event.getRecordSequence(),
                        event.getSourcePartitionTokens(), event.isFirstMoveIn()));
    }

    private void processEvent(WindowAdvancedEvent event) throws InterruptedException {
        // Entirely fast: pure in-memory state update, no I/O. Runs on event-processor thread.
        performOperation(new WindowAdvancedOperation(
                event.getToken(), event.getProcessedTimestamp(), event.getLastBoundaryRecordSequence()));
    }

    private void processSyncEvent() throws InterruptedException {
        // Fast state-machine phase: runs on the event-processor thread.
        TaskSyncContext taskSyncContext = performOperation(
                new ClearSharedPartitionOperation(),
                new TakeSharedPartitionOperation(),
                new FindPartitionForStreamingOperation(changeStream.isMutableKeyRange()),
                new RemoveFinishedPartitionOperation(spannerEventDispatcher, connectorConfig),
                new ConnectorEndDetectionOperation(finishingHandler, connectorConfig.endTime()));

        // Blocking offset-fetch + stream-submission phase: offloaded to dedicated executor.
        schedulePendingPartitionsAsync();
        failOverloadedTaskByTimer(taskSyncContext);
    }

    /**
     * Submits a {@link TakePartitionForStreamingOperation} to the dedicated
     * {@code partitionSchedulingExecutor}, unless one is already pending. This decouples the
     * blocking Kafka offset lookup from the event-processor thread, ensuring
     * {@link WindowAdvancedEvent} processing is never starved by a slow offset fetch.
     *
     * <p>The {@link #schedulingPending} flag coalesces bursts: if an event arrives while a
     * scheduling task is already queued but not yet running, the duplicate trigger is dropped —
     * the queued task will read current state when it runs and will pick up all
     * READY_FOR_STREAMING partitions. Once a task starts executing it clears the flag so the
     * next trigger can enqueue another round.
     */
    private void schedulePendingPartitionsAsync() {
        if (schedulingPending.compareAndSet(false, true)) {
            partitionSchedulingExecutor.submit(() -> {
                // Clear the flag as soon as we start so that triggers arriving during the
                // (potentially slow) offset fetch can enqueue one more task.
                schedulingPending.set(false);
                TaskSyncContext ctx = taskSyncContextHolder.get();
                String taskUid = ctx != null ? ctx.getTaskUid() : "unknown";
                try {
                    performOperation(new TakePartitionForStreamingOperation(changeStream, partitionFactory));
                }
                catch (InterruptedException e) {
                    LOGGER.info("Task {}, partition scheduling thread interrupted", taskUid);
                    Thread.currentThread().interrupt();
                }
                catch (Throwable t) {
                    LOGGER.error("Task {}, partition scheduling failed", taskUid, t);
                    RuntimeException exception = t instanceof RuntimeException
                            ? (RuntimeException) t
                            : new SpannerConnectorException("Partition scheduling failed", t);
                    errorHandler.accept(exception);
                }
            });
        }
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

        TaskSyncContext taskSyncContext = taskSyncContextHolder.updateAndGet(context -> {
            TaskSyncContext newContext = context;
            for (Operation operation : operations) {
                long nowMillis = Instant.now().toEpochMilli();
                newContext = operation.doOperation(newContext);
                if (operation.isRequiredPublishSyncEvent()) {
                    LOGGER.debug("Task {} - need to publish sync event for operation {}",
                            taskSyncContextHolder.get().getTaskUid(), operation.getClass().getSimpleName());
                    publishTaskSyncEvent.set(true);
                }
                long thenMillis = Instant.now().toEpochMilli();
                LOGGER.debug("Task {} - did operation {} in {} millis",
                        taskSyncContextHolder.get().getTaskUid(), operation.getClass().getSimpleName(), thenMillis - nowMillis);
            }
            return newContext;
        });

        if (publishTaskSyncEvent.get()) {
            taskSyncPublisher.send(taskSyncContext.buildCurrentTaskSyncEvent());
        }

        return taskSyncContext;
    }

}
