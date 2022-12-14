/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import org.slf4j.Logger;

import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.TaskStateChangeQueueUpdateMetricEvent;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;

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

    private Thread createEventHandlerThread() {
        Thread thread = new Thread(() -> {
            while (!Thread.interrupted()) {
                TaskStateChangeEvent event;
                try {
                    LOGGER.debug("createEventHandlerThread: Wait for sync event");
                    event = this.queue.take();

                    metricsEventPublisher.publishMetricEvent(new TaskStateChangeQueueUpdateMetricEvent(queue.remainingCapacity()));

                    LoggerUtils.debug(LOGGER, "createEventHandlerThread: Received sync event of type: {}, event: {}",
                            event.getClass().getSimpleName(), event);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                taskSyncContextHolder.awaitNewEpoch();

                this.taskSyncContextHolder.lock();
                try {
                    this.taskStateChangeEventHandler.processEvent(event);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    this.taskSyncContextHolder.unlock();
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
    }

    public void stopProcessing() {
        if (thread != null) {
            this.queue.clear();

            this.thread.interrupt();
            this.thread = null;
        }
    }

    public void processEvent(TaskStateChangeEvent event) {
        queue.add(event);
        metricsEventPublisher.publishMetricEvent(new TaskStateChangeQueueUpdateMetricEvent(queue.remainingCapacity()));
    }

}
