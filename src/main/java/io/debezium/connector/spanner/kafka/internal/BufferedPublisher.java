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

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * This class allows to publish the latest buffered value
 * once per time period, except the case: if the value is required
 * to be published immediately.
 */
public class BufferedPublisher<V> {

    private static final Logger LOGGER = getLogger(BufferedPublisher.class);

    private final Thread thread;
    private final AtomicReference<V> value = new AtomicReference<>();
    private final Predicate<V> publishImmediately;
    private final Consumer<V> onPublish;
    private final String taskUid;

    public BufferedPublisher(String taskUid, String name, long timeout, Predicate<V> publishImmediately, Consumer<V> onPublish) {
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
                    return;
                }
            }
        }, "SpannerConnector-" + name);
    }

    public void buffer(V update) {
        if (publishImmediately.test(update)) {
            synchronized (this) {
                // We publish without affecting the updated values.
                this.onPublish.accept(update);
            }
        }
        else {
            TaskSyncEvent toMerge = (TaskSyncEvent) update;
            TaskState toMergeTask = toMerge.getTaskStates().get(toMerge.getTaskUid());
            TaskSyncEvent existing = (TaskSyncEvent) value.get();
            TaskState existingTask = existing.getTaskStates().get(toMerge.getTaskUid());

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
            TaskState finalTaskState = toMergeTask.builder().partitions(finalOwnedPartitions)
                    .sharedPartitions(finalSharedPartitions).build();

            Map<String, TaskState> taskStates = new HashMap<>(toMerge.getTaskStates());
            taskStates.remove(toMergeTask.getTaskUid());
            // Put the final task state into the merged task sync event.
            taskStates.put(toMergeTask.getTaskUid(), finalTaskState);
            TaskSyncEvent mergedSyncEvent = toMerge.builder().taskStates(taskStates).build();
            value.set((V) mergedSyncEvent);
        }
    }

    private synchronized void publishBuffered() {
        V item = this.value.getAndSet(null);

        if (item != null) {
            this.onPublish.accept(item);
        }
    }

    public void start() {
        this.thread.start();
    }

    public void close() {
        thread.interrupt();
        while (!thread.getState().equals(Thread.State.TERMINATED)) {
        }
    }
}
