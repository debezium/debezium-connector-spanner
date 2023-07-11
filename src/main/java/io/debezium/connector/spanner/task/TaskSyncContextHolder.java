/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;

import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.TaskSyncContextMetricEvent;
import io.debezium.connector.spanner.task.utils.TimeoutMeter;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Holds the current state of the connector's task.
 * Publishes metric events, when state is changed.
 */
public class TaskSyncContextHolder {
    private static final Logger LOGGER = getLogger(TaskSyncContextHolder.class);

    private final MetricsEventPublisher metricsEventPublisher;
    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicReference<TaskSyncContext> taskSyncContextRef = new AtomicReference<>();

    private final Duration sleepInterval = Duration.ofMillis(100);
    private final Clock clock;

    public TaskSyncContextHolder(MetricsEventPublisher metricsEventPublisher) {
        this.metricsEventPublisher = metricsEventPublisher;
        this.clock = Clock.system();
    }

    public final void init(TaskSyncContext taskSyncContext) {
        taskSyncContextRef.set(taskSyncContext);
        metricsEventPublisher.publishMetricEvent(new TaskSyncContextMetricEvent(taskSyncContext));
    }

    public TaskSyncContext get() {
        return taskSyncContextRef.get();
    }

    public void update(UnaryOperator<TaskSyncContext> updateFunction) {
        this.updateAndGet(updateFunction);
    }

    public TaskSyncContext updateAndGet(UnaryOperator<TaskSyncContext> updateFunction) {
        boolean needToLock = !lock.isHeldByCurrentThread();
        if (needToLock) {
            lock.lock();
        }
        TaskSyncContext taskSyncContext;
        try {
            taskSyncContext = taskSyncContextRef.updateAndGet(updateFunction);
        }
        finally {
            if (needToLock) {
                lock.unlock();
            }
        }

        metricsEventPublisher.publishMetricEvent(new TaskSyncContextMetricEvent(taskSyncContext));

        return taskSyncContext;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void awaitInitialization(Duration awaitTimeout) {
        LOGGER.info("awaitInitialization: start");
        TimeoutMeter timeout = TimeoutMeter.setTimeout(awaitTimeout);
        while (RebalanceState.START_INITIAL_SYNC.equals(this.get().getRebalanceState())) {
            if (timeout.isExpired()) {
                LOGGER.info("Await task initialization timeout expired");
                throw new SpannerConnectorException("Await task initialization timeout expired");
            }
        }
        LOGGER.debug("awaitInitialization: end");
    }

    public void awaitNewEpoch() {
        while (!RebalanceState.NEW_EPOCH_STARTED.equals(this.get().getRebalanceState())) {
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                return;
            }
            final Metronome metronome = Metronome.sleeper(sleepInterval, clock);

            try {
                // Sleep for sleepInterval.
                metronome.pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
