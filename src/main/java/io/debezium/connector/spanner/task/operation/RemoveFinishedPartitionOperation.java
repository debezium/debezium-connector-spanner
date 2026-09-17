/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;

import com.google.cloud.Timestamp;

import io.debezium.DebeziumException;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.connector.spanner.context.offset.PartitionOffset;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/** Remove finished partition from the task state, as it is not needed anymore */
public class RemoveFinishedPartitionOperation implements Operation {
    private static final Logger LOGGER = getLogger(RemoveFinishedPartitionOperation.class);

    private final SpannerEventDispatcher spannerEventDispatcher;
    private final SpannerConnectorConfig connectorConfig;
    private boolean isRequiredPublishSyncEvent = false;

    public RemoveFinishedPartitionOperation(SpannerEventDispatcher spannerEventDispatcher, SpannerConnectorConfig spannerConnectorConfig) {
        this.spannerEventDispatcher = spannerEventDispatcher;
        this.connectorConfig = spannerConnectorConfig;
    }

    private TaskSyncContext removeFinishedPartitions(TaskSyncContext taskSyncContext) {

        TaskState taskState = taskSyncContext.getCurrentTaskState();

        List<PartitionState> partitions = taskState.getPartitions().stream()
                .map(
                        partitionState -> {
                            if (partitionState.getState().equals(PartitionStateEnum.FINISHED)) {
                                if (partitionState.getFinishedTimestamp() == null) {
                                    throw new DebeziumException(
                                            "FinishedTimestamp must be specified for finished partitions");
                                }
                                Timestamp deletionTime = Timestamp.ofTimeSecondsAndNanos(
                                        partitionState.getFinishedTimestamp().getSeconds()
                                                + connectorConfig.getFinishedPartitionDeletionDelay().getSeconds(),
                                        0);
                                Timestamp currentTime = Timestamp.now();

                                if (deletionTime.compareTo(currentTime) < 0) {

                                    List<PartitionState> allPartitionStates = allPartitionStates(taskSyncContext);

                                    if (allChildrenFinished(allPartitionStates, partitionState.getToken())
                                            && moveOutDestinationsHaveResumed(allPartitionStates, partitionState)) {
                                        LOGGER.info(
                                                "Partition {} will be removed from the task with finished timestamp {},"
                                                        + " deletion timestamp {} and current time {}",
                                                partitionState,
                                                partitionState.getFinishedTimestamp(),
                                                deletionTime,
                                                currentTime);

                                        LOGGER.info("Task {}, Dispatching null offset for partition {} because it is removed", taskSyncContext.getTaskUid(),
                                                partitionState.getToken());
                                        PartitionOffset partitionOffset = new PartitionOffset();
                                        SpannerOffsetContext spannerOffsetContext = new SpannerOffsetContext(partitionOffset, new TransactionContext());
                                        SpannerPartition partition = new SpannerPartition(partitionState.getToken());
                                        try {
                                            spannerEventDispatcher.alwaysDispatchHeartbeatEvent(partition, spannerOffsetContext);
                                        }
                                        catch (InterruptedException e) {
                                            LOGGER.error("Task {}, Failed to send null offset for partition {}", taskSyncContext.getTaskUid(), partitionState.getToken());
                                        }
                                        return null;
                                    }
                                    else {
                                        LOGGER.info("Task {}, waiting to remove partition {}", taskSyncContext.getTaskUid(), partitionState);
                                    }
                                }

                                return partitionState;
                            }
                            return partitionState;
                        })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (taskState.getPartitions().size() != partitions.size()) {
            this.isRequiredPublishSyncEvent = true;
        }
        return taskSyncContext.toBuilder()
                .currentTaskState(taskState.toBuilder().partitions(partitions).build())
                .build();
    }

    private static List<PartitionState> allPartitionStates(TaskSyncContext taskSyncContext) {
        return Stream.concat(
                Stream.concat(
                        taskSyncContext.getTaskStates().values().stream()
                                .flatMap(taskState -> taskState.getPartitions().stream()),
                        taskSyncContext.getCurrentTaskState().getPartitions().stream()),
                Stream.concat(
                        taskSyncContext.getTaskStates().values().stream()
                                .flatMap(taskState -> taskState.getSharedPartitions().stream()),
                        taskSyncContext.getCurrentTaskState().getSharedPartitions().stream()))
                .collect(Collectors.toList());
    }

    private static boolean allChildrenFinished(List<PartitionState> allPartitionStates, String token) {
        Set<String> children = allPartitionStates.stream()
                .filter(partitionState -> partitionState.getParents().contains(token))
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());

        return children.isEmpty()
                || children.stream()
                        .allMatch(
                                childToken -> {
                                    return allPartitionStates.stream()
                                            .filter(partitionState -> childToken.equals(partitionState.getToken()))
                                            .allMatch(
                                                    partitionState -> PartitionStateEnum.FINISHED.equals(partitionState.getState())
                                                            || PartitionStateEnum.REMOVED.equals(partitionState.getState()));
                                });
    }

    /**
     * Mutable key range only: a partition that recorded a {@link MoveOutState} must not be
     * deleted (losing that state) until every destination it lists has itself streamed past the
     * move's commit timestamp. A destination that hasn't yet reached that point in its own
     * stream has not discovered the dependency at all - its {@code moveInState} is still
     * {@code null}, indistinguishable from "already resumed" - so presence/absence of
     * {@code moveInState} alone cannot be used here. Comparing against the destination's own
     * {@code processedTimestamp} is the only reliable signal. Once a destination's
     * {@code processedTimestamp} reaches the move's timestamp, {@link MoveInStateUpdateOperation}
     * has, by construction, already updated its {@code parents} to include this source token in
     * that same step, so {@link #allChildrenFinished} takes over correctly from that point on.
     *
     * <p>A source partition never pauses for its own MoveOut events, so it can accumulate
     * several independent, still-pending {@link MoveOutState} entries (to different destinations,
     * at different commit timestamps) over its lifetime - see {@code getMoveOutStates()}. Every
     * entry must individually be resolved before the source can be deleted.
     *
     * <p>This is purely additive for the immutable key range path: partitions created via
     * {@link ChildPartitionOperation} never populate {@code moveOutStates}, so this check
     * trivially returns {@code true} and the deletion condition is unchanged from before
     * mutable key range support was added.
     */
    private static boolean moveOutDestinationsHaveResumed(List<PartitionState> allPartitionStates, PartitionState partitionState) {
        for (MoveOutState moveOutState : partitionState.getMoveOutStates()) {
            Timestamp moveOutTimestamp = moveOutState.getTimestamp();
            for (String destToken : moveOutState.getDestPartitionTokens()) {
                PartitionState dest = allPartitionStates.stream()
                        .filter(p -> destToken.equals(p.getToken()))
                        .findFirst()
                        .orElse(null);
                if (dest == null) {
                    // Destination not tracked anywhere - nothing left depending on this source.
                    continue;
                }
                boolean destHasReachedThisMove = dest.getProcessedTimestamp() != null
                        && dest.getProcessedTimestamp().compareTo(moveOutTimestamp) >= 0;
                if (!destHasReachedThisMove) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return removeFinishedPartitions(taskSyncContext);
    }
}
