/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Verifies that processing a MoveIn event advances the destination partition's
 * {@code processedTimestamp}/{@code lastBoundaryRecordSequence} to the exact MoveIn boundary,
 * so that once the partition resumes it starts from that boundary instead of re-querying (and
 * re-emitting) the range between its old processedTimestamp and the MoveIn's commit timestamp.
 */
class MoveInStateUpdateOperationTest {

    private static final Timestamp OLD_PROCESSED_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(100, 0);
    private static final Timestamp MOVE_IN_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(1000, 0);

    @Test
    void movesProcessedTimestampAndRecordSequenceToTheMoveInBoundary() {
        PartitionState destPartition = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of("originalParent"))
                .processedTimestamp(OLD_PROCESSED_TIMESTAMP)
                .build();

        TaskSyncContext context = TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(destPartition))
                        .sharedPartitions(List.of())
                        .build())
                .build();

        TaskSyncContext result = new MoveInStateUpdateOperation(
                "dst", MOVE_IN_TIMESTAMP, "00042", List.of("src1")).doOperation(context);

        PartitionState updated = result.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals("dst"))
                .findFirst()
                .orElseThrow();

        assertEquals(MOVE_IN_TIMESTAMP, updated.getProcessedTimestamp(),
                "processedTimestamp must jump to the MoveIn commit timestamp so streaming resumes at the exact boundary");
        assertEquals("00042", updated.getLastBoundaryRecordSequence(),
                "lastBoundaryRecordSequence must be recorded so filterBoundaryDuplicates can skip already-seen records at that timestamp");
        assertEquals(PartitionStateEnum.CREATED, updated.getState());
        assertEquals(Set.of("src1"), updated.getParents());
    }

    /**
     * A partition can be a MoveOut source and a MoveIn destination independently (e.g. it gives
     * away one sub-range while receiving another). Processing this MoveIn must not wipe out an
     * unrelated, still-pending {@code moveOutStates} entry - {@link RemoveFinishedPartitionOperation}
     * relies on it to avoid deleting this partition before its own destination catches up.
     */
    @Test
    void doesNotClearUnrelatedPendingMoveOutState() {
        MoveOutState existingMoveOutState = new MoveOutState(Timestamp.ofTimeSecondsAndNanos(50, 0), List.of("otherDest"));
        PartitionState destPartition = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of("originalParent"))
                .processedTimestamp(OLD_PROCESSED_TIMESTAMP)
                .moveOutStates(List.of(existingMoveOutState))
                .build();

        TaskSyncContext context = TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(destPartition))
                        .sharedPartitions(List.of())
                        .build())
                .build();

        TaskSyncContext result = new MoveInStateUpdateOperation(
                "dst", MOVE_IN_TIMESTAMP, "00042", List.of("src1")).doOperation(context);

        PartitionState updated = result.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals("dst"))
                .findFirst()
                .orElseThrow();

        assertFalse(updated.getMoveOutStates().isEmpty(), "an unrelated pending moveOutStates entry must survive processing of this partition's MoveIn");
        assertEquals(List.of(existingMoveOutState), updated.getMoveOutStates());
    }
}
