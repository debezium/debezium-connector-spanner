/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;

import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * This class allows to publish the latest buffered value
 * once per time period, except the case: if the value is required
 * to be published immediately.
 */
public class BufferedPublisher<V> {

    private static final Logger LOGGER = getLogger(BufferedPublisher.class);

    private volatile Thread thread;
    private final AtomicReference<V> value = new AtomicReference<>();
    private final Predicate<V> publishImmediately;
    private final Consumer<V> onPublish;
    private final String taskUid;

    private final Duration sleepInterval = Duration.ofMillis(100);
    private final Clock clock;
    private final TaskSyncContextHolder taskSyncContextHolder;

    public BufferedPublisher(String taskUid, String name, TaskSyncContextHolder taskSyncContextHolder, long timeout, Predicate<V> publishImmediately,
                             Consumer<V> onPublish) {
        this.publishImmediately = publishImmediately;
        this.onPublish = onPublish;
        this.taskUid = taskUid;
        this.clock = Clock.system();
        this.taskSyncContextHolder = taskSyncContextHolder;

        this.thread = new Thread(() -> {
            Instant lastUpdatedTime = Instant.now();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (Instant.now().isAfter(lastUpdatedTime.plus(Duration.ofSeconds(600)))) {
                        LOGGER.info(
                                "Task Uid {} is still publishing with AtomicReference value {}",
                                this.taskUid,
                                (this.value.get() == null));
                        lastUpdatedTime = Instant.now();
                    }
                    publishBuffered();
                    Thread.sleep(timeout);
                }
                catch (InterruptedException e) {
                    LOGGER.info("Task Uid {}, publishing thread caught interrupt", this.taskUid);
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "SpannerConnector-" + name);
    }

    public void buffer(V update) {
        if (publishImmediately.test(update)) {
            synchronized (this) {
                this.value.set(null);
                this.onPublish.accept(update);
            }
            if (update.getClass() == TaskSyncEvent.class) {
                TaskSyncEvent event = (TaskSyncEvent) update;

                LOGGER.debug("Task {}, publishing event with rebalance generation ID {} and message type {}", this.taskUid,
                        event.getRebalanceGenerationId(),
                        event.getMessageType());
            }
        }
        else {
            value.set(update);
        }
    }

    private void publishBuffered() {
        TaskSyncContext context = taskSyncContextHolder.get();
        if (context == null || context.getRebalanceState() != RebalanceState.NEW_EPOCH_STARTED) {
            return;
        }
        V item = this.value.getAndSet(null);

        if (item != null) {
            if (item.getClass() == TaskSyncEvent.class) {
                TaskSyncEvent event = (TaskSyncEvent) item;

                LOGGER.debug("Task {}, publishing buffered event with rebalance generation ID {} and message type {} and context rebalance state {}", this.taskUid,
                        event.getRebalanceGenerationId(),
                        event.getMessageType(), context.getRebalanceState());
            }
            this.onPublish.accept(item);
        }
    }

    public void start() {
        this.thread.start();
    }

    public void close() {
        LOGGER.info(
                "Stopping BufferedPublisher for Task Uid {}",
                this.taskUid,
                (this.value.get() == null));
        if (thread == null) {
            LOGGER.info(
                    "BufferedPublisher thread is already terminated for Task Uid {}",
                    this.taskUid,
                    (this.value.get() == null));
            return;
        }
        thread.interrupt();
        final Metronome metronome = Metronome.sleeper(sleepInterval, clock);
        while (!thread.getState().equals(Thread.State.TERMINATED)) {
            try {
                // Sleep for sleepInterval.
                LOGGER.info(
                        "Still stopping BufferedPublisher for Task Uid {}",
                        this.taskUid);
                thread.interrupt();

                metronome.pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        this.thread = null;
        LOGGER.info(
                "Stopped BufferedPublisher for Task Uid {}",
                this.taskUid,
                (this.value.get() == null));
    }
}
