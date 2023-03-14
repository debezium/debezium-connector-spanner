/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * This class allows to publish the latest buffered value
 * once per time period, except the case: if the value is required
 * to be published immediately.
 */
public class BufferedPublisher {

    private static final Logger LOGGER = getLogger(BufferedPublisher.class);

    private final Thread thread;
    private final AtomicReference<TaskSyncEvent> value = new AtomicReference<TaskSyncEvent>();
    private final Predicate<TaskSyncEvent> publishImmediately;
    private final Consumer<TaskSyncEvent> onPublish;
    private final String taskUid;

    public BufferedPublisher(String taskUid, String name, long timeout, Predicate<TaskSyncEvent> publishImmediately, Consumer<TaskSyncEvent> onPublish) {
        this.publishImmediately = publishImmediately;
        this.onPublish = onPublish;
        this.taskUid = taskUid;

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
                    LOGGER.info("Task Uid {} caught interrupted exception", taskUid);
                    return;
                }
            }
            LOGGER.info("Terminating this thread now for task {}", taskUid);
        }, "SpannerConnector-" + name);
    }

    public synchronized void buffer(TaskSyncEvent update) {
        if (publishImmediately.test(update)) {
            // We publish without affecting the updated values.
            this.onPublish.accept(update);
        }
        else {
            TaskSyncEvent updatedValue = value.updateAndGet(
                    originalValue -> mergeTaskSyncEvent(taskUid, originalValue, update));
            LOGGER.info("Updated value: {} and retrieved value {}", updatedValue, value.get());
        }
    }

    public static TaskSyncEvent mergeTaskSyncEvent(String task, TaskSyncEvent originalValue, TaskSyncEvent newValue) {
        TaskSyncEvent toMerge = newValue;
        LOGGER.info("TaskUID: {}", toMerge.getTaskUid());
        TaskSyncEvent existing = originalValue;
        TaskState toMergeTask = toMerge.getTaskStates().get(task);

        if (toMergeTask == null) {
            if (existing == null) {
                return null;
            }
            LOGGER.warn("TaskSyncEvent does not contain the task uid {}, {}", task, toMerge);
            return existing;
        }

        if (existing == null) {
            LOGGER.info("Buffered existing value: {}", toMerge);
            return toMerge;
        }

        TaskState existingTask = existing.getTaskStates().get(task);
        LOGGER.info("To merge: {}", toMerge);
        LOGGER.info("Existing: {}", existing);

        // Get a list of partition tokens that are newly modified by new message
        List<String> newlyOwnedPartitions = toMergeTask.getPartitions().stream()
                .map(partitionState -> partitionState.getToken())
                .collect(Collectors.toList());

        // Get a list of partition tokens that are newly shared by new message
        List<String> newlySharedPartitions = toMergeTask.getSharedPartitions().stream()
                .map(partitionState -> partitionState.getToken())
                .collect(Collectors.toList());

        // Get a list of partition tokens that the old message contained that the new message
        // didn't
        List<PartitionState> previouslyOwnedPartitions = existingTask.getPartitions().stream()
                .filter(partitionState -> !newlyOwnedPartitions.contains(
                        partitionState.getToken()))
                .collect(Collectors.toList());
        // Get a list of shared partition tokens that the old message contained that the
        // new message didn't
        List<PartitionState> previouslySharedPartitions = existingTask.getSharedPartitions().stream()
                .filter(partitionState -> !newlySharedPartitions.contains(
                        partitionState.getToken()))
                .collect(Collectors.toList());

        // Get the total list of shared + modified tokens.
        List<PartitionState> finalOwnedPartitions = toMergeTask.getPartitions().stream()
                .collect(Collectors.toList());
        finalOwnedPartitions.addAll(previouslyOwnedPartitions);
        List<PartitionState> finalSharedPartitions = toMergeTask.getSharedPartitions().stream()
                .collect(Collectors.toList());

        finalSharedPartitions.addAll(previouslySharedPartitions);

        // Put the total list of shared + modified tokens into the final task state.
        TaskState finalTaskState = toMergeTask.toBuilder().partitions(finalOwnedPartitions)
                .sharedPartitions(finalSharedPartitions).build();

        Map<String, TaskState> taskStates = new HashMap<>(toMerge.getTaskStates());
        taskStates.remove(toMergeTask.getTaskUid());
        // Put the final task state into the merged task sync event.
        taskStates.put(task, finalTaskState);
        TaskSyncEvent mergedSyncEvent = toMerge.toBuilder().taskStates(taskStates).build();
        LOGGER.info("Buffered merged value: {} with toMerge {} and existing {}", mergedSyncEvent, toMerge, existing);
        return mergedSyncEvent;
    }

    @VisibleForTesting
    public synchronized void publishBuffered() {
        TaskSyncEvent item = this.value.getAndSet(null);

        if (item != null) {
            LOGGER.info("Publishing buffered: {}", item);
            this.onPublish.accept(item);
        }
    }

    public void start() {
        LOGGER.info("Starting buffered publisher for {}", taskUid);
        this.thread.start();
    }

    public void close() {
        LOGGER.info("Closing buffered publisher for {}", taskUid);
        thread.interrupt();
        while (!thread.getState().equals(Thread.State.TERMINATED)) {
        }
    }
}
