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

    public String lockDebugString() {
        return "Lock Debug String {is locked: " + lock.isLocked() + ", isLockedByCurrentThread: " + lock.isHeldByCurrentThread() + ", lock debug string: "
                + lock.toString() + "current thread " + Thread.currentThread().getName();
    }

    public void update(UnaryOperator<TaskSyncContext> updateFunction) {
        this.updateAndGet(updateFunction);
    }

    public TaskSyncContext updateAndGet(UnaryOperator<TaskSyncContext> updateFunction) {
        TaskSyncContext taskSyncContext;
        try {
            LOGGER.info("Task {}, trying to lock TaskSyncContext, lock debug string {}", get().getTaskUid(), lockDebugString());
            lock.lock();
            LOGGER.info("Task {}, locked TaskSyncContext, lock debug string {}", get().getTaskUid(), lockDebugString());
            taskSyncContext = taskSyncContextRef.updateAndGet(updateFunction);
            LOGGER.info("Task {}, updated TaskSyncContext, lock debug string {}", get().getTaskUid(), lockDebugString());
        }
        finally {
            if (lock.isHeldByCurrentThread()) {
                LOGGER.info("Task {}, unlocking TaskSyncContext, lock debug string {}", get().getTaskUid(), lockDebugString());
                lock.unlock();
                LOGGER.info("Task {}, unlocked TaskSyncContext, lock debug string {}", get().getTaskUid(), lockDebugString());
            }
        }

        metricsEventPublisher.publishMetricEvent(new TaskSyncContextMetricEvent(taskSyncContext));

        return taskSyncContext;
    }

    public void awaitInitialization(Duration awaitTimeout) {
        LOGGER.info("Task {} awaitInitialization: start", get().getTaskUid());
        TimeoutMeter timeout = TimeoutMeter.setTimeout(awaitTimeout);
        while (RebalanceState.START_INITIAL_SYNC.equals(this.get().getRebalanceState())) {
            if (timeout.isExpired()) {
                LOGGER.info("Await task initialization timeout expired");
                throw new SpannerConnectorException("Await task initialization timeout expired");
            }
        }
        LOGGER.info("Task {} awaitInitialization: end", get().getTaskUid());
    }

    public void awaitNewEpoch() {
        LOGGER.info("Task {} awaitNewEpoch: start", get().getTaskUid());
        while (!RebalanceState.NEW_EPOCH_STARTED.equals(this.get().getRebalanceState())) {
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                return;
            }
            final Metronome metronome = Metronome.sleeper(sleepInterval, clock);

            LOGGER.info("Task {} still awaiting new epoch: start, rebalance state {}", get().getTaskUid(), get().getRebalanceState());
            try {
                // Sleep for sleepInterval.
                metronome.pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info("Task {} awaitNewEpoch: end", get().getTaskUid());
    }

}