/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveInState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Persists the MoveIn state for a destination partition that has just processed a
 * MoveIn event and is pausing to wait until the source partition(s) have processed
 * their corresponding MoveOut event(s). The partition is moved back to
 * {@link PartitionStateEnum#CREATED}, its parents are updated to the source partition
 * tokens, and the {@code processedTimestamp}/{@code lastBoundaryRecordSequence} fields
 * are set so that streaming resumes from the exact MoveIn boundary without re-emitting
 * already-processed records.
 *
 * <p>A partition can independently take part in a MoveOut as a source and a MoveIn as a
 * destination (e.g. it gives away one sub-range while receiving another). Any existing
 * {@code moveOutState} on this partition is therefore left untouched here - it tracks an
 * unrelated pending move-out that {@link RemoveFinishedPartitionOperation} still needs in
 * order to avoid deleting this partition before its own destination(s) catch up, and must
 * not be wiped out just because this partition also happens to be processing a MoveIn.
 */
public class MoveInStateUpdateOperation implements Operation {

    private final String token;
    private final Timestamp commitTimestamp;
    private final String recordSequence;
    private final List<String> sourcePartitionTokens;

    public MoveInStateUpdateOperation(String token, Timestamp commitTimestamp, String recordSequence, List<String> sourcePartitionTokens) {
        this.token = token;
        this.commitTimestamp = commitTimestamp;
        this.recordSequence = recordSequence;
        this.sourcePartitionTokens = sourcePartitionTokens;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return true;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        MoveInState newMoveInState = new MoveInState(commitTimestamp, recordSequence, sourcePartitionTokens);

        List<PartitionState> updatedPartitions = currentTaskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getToken().equals(token)) {
                        return partitionState.toBuilder()
                                .state(PartitionStateEnum.CREATED)
                                .parents(new HashSet<>(sourcePartitionTokens))
                                .moveInState(newMoveInState)
                                .processedTimestamp(commitTimestamp)
                                .lastBoundaryRecordSequence(recordSequence)
                                .build();
                    }
                    return partitionState;
                })
                .collect(Collectors.toList());

        return taskSyncContext.toBuilder()
                .currentTaskState(currentTaskState.toBuilder().partitions(updatedPartitions).build())
                .build();
    }
}
