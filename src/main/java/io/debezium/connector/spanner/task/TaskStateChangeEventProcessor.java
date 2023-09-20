/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import org.slf4j.Logger;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.TaskStateChangeQueueUpdateMetricEvent;
import io.debezium.connector.spanner.task.state.NewPartitionsEvent;
import io.debezium.connector.spanner.task.state.SyncEvent;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Owns queue of {@link TaskStateChangeEvent} elements,
 * polls them in the separate thread and sends them to
 * {@link TaskStateChangeEventHandler} for further processing
 */
public class TaskStateChangeEventProcessor {

    private static final Logger LOGGER = getLogger(TaskStateChangeEventProcessor.class);

    private final BlockingQueue<TaskStateChangeEvent> queue;

    private final TaskSyncContextHolder taskSyncContextHolder;

    private final TaskStateChangeEventHandler taskStateChangeEventHandler;

    private final Consumer<Throwable> errorHandler;

    private final MetricsEventPublisher metricsEventPublisher;

    private volatile Thread thread;
    private volatile Thread eventQueueingThread;
    private final Duration sleepInterval = Duration.ofMillis(100);

    public TaskStateChangeEventProcessor(int queueCapacity, TaskSyncContextHolder taskSyncContextHolder,
                                         TaskStateChangeEventHandler taskStateChangeEventHandler,
                                         Consumer<Throwable> errorHandler,
                                         MetricsEventPublisher metricsEventPublisher) {
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.taskSyncContextHolder = taskSyncContextHolder;
        this.errorHandler = errorHandler;
        this.taskStateChangeEventHandler = taskStateChangeEventHandler;

        this.metricsEventPublisher = metricsEventPublisher;
    }

    private Thread createEventQueueingThread() {
        Thread thread = new Thread(() -> {
            LOGGER.info("Started Event Queueing Thread");
            Clock clock = Clock.system();
            final Metronome metronome = Metronome.sleeper(Duration.ofSeconds(5), clock);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (taskSyncContextHolder.get() == null) {
                        continue;
                    }
                    LOGGER.info("Task {}, continuing Event Queueing Thread",
                            taskSyncContextHolder.get().getTaskUid());
                    this.queue.put(new SyncEvent());
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    LOGGER.error("Task interrupting event queueing thread");
                    Thread.currentThread().interrupt();
                    return;
                }
                catch (Exception e) {
                    LOGGER.error("Task caught exception from event queueing thread");
                }
            }
        });
        thread.setUncaughtExceptionHandler((t, e) -> errorHandler.accept(e));
        return thread;
    }

    private Thread createEventHandlerThread() {
        Thread thread = new Thread(() -> {
            while (!Thread.interrupted()) {
                TaskStateChangeEvent event;
                try {
                    LOGGER.debug("createEventHandlerThread: Wait for sync event");

                    event = this.queue.take();

                    metricsEventPublisher.publishMetricEvent(new TaskStateChangeQueueUpdateMetricEvent(queue.remainingCapacity()));

                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.info("Task {}, interrupting the event handler thread", this.taskSyncContextHolder.get().getTaskUid());
                    return;
                }

                taskSyncContextHolder.awaitNewEpoch();

                try {
                    this.taskStateChangeEventHandler.processEvent(event);
                }
                catch (InterruptedException e) {
                    LOGGER.info("Task {}, interrupting the event handler thread", this.taskSyncContextHolder.get().getTaskUid());
                    Thread.currentThread().interrupt();
                }
            }
        }, "SpannerConnector-TaskStateChangeEventProcessor");

        thread.setUncaughtExceptionHandler((t, e) -> errorHandler.accept(e));

        return thread;
    }

    public void startProcessing() {
        if (thread != null) {
            return;
        }
        this.thread = createEventHandlerThread();
        this.thread.start();
        this.eventQueueingThread = createEventQueueingThread();
        this.eventQueueingThread.start();
    }

    public void stopProcessing() {
        if (this.eventQueueingThread != null) {
            Clock clock = Clock.system();
            final Metronome metronome = Metronome.sleeper(sleepInterval, clock);
            LOGGER.info("Task {}, stopping event queueing thread ", this.taskSyncContextHolder.get().getTaskUid());
            while (!this.eventQueueingThread.getState().equals(Thread.State.TERMINATED)) {
                try {
                    // Sleep for sleepInterval.
                    metronome.pause();

                    this.eventQueueingThread.interrupt();

                    LOGGER.info("Task {}, still waiting for event queueing thread to die", this.taskSyncContextHolder.get().getTaskUid());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            this.eventQueueingThread = null;
            LOGGER.info("Task {}, killed event queueing thread ", this.taskSyncContextHolder.get().getTaskUid());
        }
        if (thread != null) {
            this.queue.clear();

            this.thread.interrupt();
            this.thread = null;
        }

    }

    public void processEvent(TaskStateChangeEvent event) throws InterruptedException {
        if (event instanceof NewPartitionsEvent) {
            NewPartitionsEvent newPartitionsEvent = (NewPartitionsEvent) event;

            List<Partition> filteredPartitions = removeAlreadyExistingPartitions(newPartitionsEvent.getPartitions());
            if (!filteredPartitions.isEmpty()) {
                queue.put(new NewPartitionsEvent(filteredPartitions));
            }
        }
        else {
            queue.put(event);
        }

        metricsEventPublisher.publishMetricEvent(new TaskStateChangeQueueUpdateMetricEvent(queue.remainingCapacity()));
    }

    private List<Partition> removeAlreadyExistingPartitions(List<Partition> partitions) {
        Set<String> existingPartitions = TaskStateUtil.allPartitionTokens(taskSyncContextHolder.get());
        return partitions.stream()
                .filter(p -> !existingPartitions.contains(p.getToken()))
                .collect(toList());
    }

}
