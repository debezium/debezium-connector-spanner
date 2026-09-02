/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveInState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Persists MoveIn state for a destination partition that is using the buffer-gate
 * optimisation path.
 *
 * <p>Unlike {@link MoveInStateUpdateOperation} this operation intentionally does
 * <em>not</em> transition the partition to {@code CREATED} and does <em>not</em> update
 * the {@code parents} set, because the streaming thread remains alive and self-gates via
 * a {@link io.debezium.connector.spanner.db.stream.MoveInBufferGate} rather than exiting
 * and relying on the state machine to restart it.
 *
 * <p>For the <em>first</em> MoveIn event in a buffer sequence ({@code isFirstMoveIn=true})
 * the {@code processedTimestamp} and {@code lastBoundaryRecordSequence} fields are updated
 * so that crash-recovery can restart the query from the correct boundary without
 * re-emitting events that were already forwarded to the downstream Kafka topic before the
 * gate activated.
 *
 * <p>For subsequent MoveIn events ({@code isFirstMoveIn=false}) only the
 * {@code moveInState} field is updated.  This keeps {@code processedTimestamp} at the
 * first MoveIn's timestamp so that crash-recovery does not skip the events between the
 * first and later MoveIn timestamps (those events were buffered, not forwarded, and must
 * therefore be re-read from Spanner on restart).
 *
 * <p>The {@code moveOutStates} field on the destination partition is left untouched: a
 * partition can simultaneously be a source for an unrelated MoveOut and a destination for
 * a MoveIn, and the source-side state must not be overwritten.
 */
public class PublishMoveInStateOperation implements Operation {

    private final String token;
    private final Timestamp commitTimestamp;
    private final String recordSequence;
    private final List<String> sourcePartitionTokens;
    private final boolean isFirstMoveIn;

    public PublishMoveInStateOperation(String token, Timestamp commitTimestamp, String recordSequence,
                                       List<String> sourcePartitionTokens, boolean isFirstMoveIn) {
        this.token = token;
        this.commitTimestamp = commitTimestamp;
        this.recordSequence = recordSequence;
        this.sourcePartitionTokens = sourcePartitionTokens;
        this.isFirstMoveIn = isFirstMoveIn;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return true;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        List<PartitionState> updatedPartitions = currentTaskState.getPartitions().stream()
                .map(partitionState -> {
                    if (!partitionState.getToken().equals(token)) {
                        return partitionState;
                    }
                    List<String> effectiveSourcePartitionTokens = sourcePartitionTokens;
                    MoveInState currentMoveInState = partitionState.getMoveInState();
                    if (!isFirstMoveIn && currentMoveInState != null && commitTimestamp.equals(currentMoveInState.getTimestamp())) {
                        effectiveSourcePartitionTokens = Stream.concat(
                                currentMoveInState.getSourcePartitionTokens().stream(), sourcePartitionTokens.stream())
                                .distinct()
                                .collect(Collectors.toList());
                    }
                    MoveInState newMoveInState = new MoveInState(commitTimestamp, recordSequence, effectiveSourcePartitionTokens);
                    PartitionState.PartitionStateBuilder builder = partitionState.toBuilder()
                            .moveInState(newMoveInState);
                    if (isFirstMoveIn) {
                        // Only the first MoveIn in a buffer sequence pins processedTimestamp,
                        // enabling boundary deduplication on crash-recovery. State enum and
                        // parents are intentionally unchanged — the thread is still alive.
                        builder.processedTimestamp(commitTimestamp)
                                .lastBoundaryRecordSequence(recordSequence);
                    }
                    return builder.build();
                })
                .collect(Collectors.toList());

        return taskSyncContext.toBuilder()
                .currentTaskState(currentTaskState.toBuilder().partitions(updatedPartitions).build())
                .build();
    }
}
