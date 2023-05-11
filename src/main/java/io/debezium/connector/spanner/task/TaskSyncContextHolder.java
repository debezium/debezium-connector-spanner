/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.Arrays;
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

    private static final Duration AWAIT_TIME_TIME_OUT = Duration.ofSeconds(120);

    private static final Logger LOGGER = getLogger(TaskSyncContextHolder.class);

    private final MetricsEventPublisher metricsEventPublisher;
    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicReference<TaskSyncContext> taskSyncContextRef = new AtomicReference<>();
    private String holder;

    private final Duration sleepInterval = Duration.ofMillis(100);
    private final Clock clock;

    public TaskSyncContextHolder(MetricsEventPublisher metricsEventPublisher) {
        this.metricsEventPublisher = metricsEventPublisher;
        this.clock = Clock.system();
        this.holder = "The thread isn't locked in the current moment.";
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
        Thread currentThread = Thread.currentThread();
        String stackTrace = Arrays.toString(Thread.currentThread().getStackTrace()).replace(',', '\n');
        holder = "Name: " + currentThread.getName() + " Stacktrace: " + stackTrace + "\n";
    }

    public boolean isLocked() {
        return lock.isLocked();
    }

    public void unlock() {
        lock.unlock();
    }

    public String getHolder() {
        if (lock.isLocked()) {
            return holder;
        }
        return "The thread isn't locked in the current moment.";
    }

    public int getHoldCount() {
        return lock.getHoldCount();

    }

    public boolean isHeldByCurrentThread() {
        return lock.isHeldByCurrentThread();
    }

    public void awaitInitialization() {
        LOGGER.debug("awaitInitialization: start");
        TimeoutMeter timeout = TimeoutMeter.setTimeout(AWAIT_TIME_TIME_OUT);
        while (RebalanceState.START_INITIAL_SYNC.equals(this.get().getRebalanceState())) {
            if (timeout.isExpired()) {
                throw new SpannerConnectorException("Await task initialization timeout expired");
            }
        }
        LOGGER.debug("awaitInitialization: end");
    }

    public void awaitNewEpoch() {
        while (!RebalanceState.NEW_EPOCH_STARTED.equals(this.get().getRebalanceState())) {
            if (Thread.interrupted()) {
                LOGGER.info("Interrupting awaitNewEpoch task {}", taskSyncContextRef.get().getTaskUid());
                Thread.currentThread().interrupt();
                return;
            }
            final Metronome metronome = Metronome.sleeper(sleepInterval, clock);

            try {
                // Sleep for sleepInterval.
                metronome.pause();
            }
            catch (InterruptedException e) {
                LOGGER.info("Interrupting awaitNewEpoch task {}", taskSyncContextRef.get().getTaskUid());
                Thread.currentThread().interrupt();
            }
        }
    }

}