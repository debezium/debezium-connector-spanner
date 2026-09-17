/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveInState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Unit tests for {@link PublishMoveInStateOperation}, specifically the source-token
 * coalescing logic introduced for partition-merge scenarios where multiple MoveIn events
 * arrive at the same commit timestamp.
 */
class PublishMoveInStateOperationTest {

    private static final Timestamp OLD_PROCESSED_TS = Timestamp.ofTimeSecondsAndNanos(100, 0);
    private static final Timestamp MOVE_IN_TS = Timestamp.ofTimeSecondsAndNanos(1000, 0);
    private static final Timestamp OTHER_TS = Timestamp.ofTimeSecondsAndNanos(2000, 0);

    // -----------------------------------------------------------------------
    // First MoveIn (isFirstMoveIn = true)
    // -----------------------------------------------------------------------

    /**
     * The first MoveIn event in a buffer sequence must pin {@code processedTimestamp} and
     * {@code lastBoundaryRecordSequence} so crash-recovery can restart at the exact boundary.
     * The partition {@code state} and {@code parents} must remain unchanged because the
     * streaming thread stays alive (unlike the close/reopen path).
     */
    @Test
    void firstMoveIn_pinsTimestampAndRecordSequence_doesNotChangeStateOrParents() {
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of("originalParent"))
                .processedTimestamp(OLD_PROCESSED_TS)
                .build();

        TaskSyncContext result = new PublishMoveInStateOperation(
                "dst", MOVE_IN_TS, "00001", List.of("src1"), true).doOperation(contextWith(dest));

        PartitionState updated = destPartition(result);

        assertEquals(MOVE_IN_TS, updated.getProcessedTimestamp(),
                "processedTimestamp must be pinned to the MoveIn boundary for crash-recovery");
        assertEquals("00001", updated.getLastBoundaryRecordSequence());
        assertEquals(List.of("src1"), updated.getMoveInState().getSourcePartitionTokens());
        assertEquals(MOVE_IN_TS, updated.getMoveInState().getTimestamp());
        // Streaming thread is still alive — state and parents must not change.
        assertEquals(PartitionStateEnum.RUNNING, updated.getState());
        assertEquals(Set.of("originalParent"), updated.getParents());
    }

    // -----------------------------------------------------------------------
    // Subsequent MoveIn (isFirstMoveIn = false) — coalescing cases
    // -----------------------------------------------------------------------

    /**
     * When a second MoveIn arrives at the same commit timestamp (partition merge), its source
     * tokens must be merged with the tokens already stored in {@code moveInState} so that
     * crash-recovery waits for all sources, not just the last one published.
     * {@code processedTimestamp} and {@code lastBoundaryRecordSequence} must not change.
     */
    @Test
    void subsequentMoveIn_sameTimestamp_coalescesSources() {
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of("originalParent"))
                .processedTimestamp(MOVE_IN_TS)
                .lastBoundaryRecordSequence("00001")
                .moveInState(new MoveInState(MOVE_IN_TS, "00001", List.of("src1")))
                .build();

        TaskSyncContext result = new PublishMoveInStateOperation(
                "dst", MOVE_IN_TS, "00002", List.of("src2"), false).doOperation(contextWith(dest));

        PartitionState updated = destPartition(result);

        List<String> sources = updated.getMoveInState().getSourcePartitionTokens();
        assertEquals(2, sources.size());
        assertTrue(sources.containsAll(List.of("src1", "src2")));
        // Boundary fields pinned by the first MoveIn must not move.
        assertEquals(MOVE_IN_TS, updated.getProcessedTimestamp());
        assertEquals("00001", updated.getLastBoundaryRecordSequence(),
                "lastBoundaryRecordSequence must remain from the first MoveIn, not be updated");
    }

    /**
     * When a subsequent MoveIn event arrives at a <em>different</em> commit timestamp, there is
     * no same-merge coalescing — the sources are replaced rather than merged.
     */
    @Test
    void subsequentMoveIn_differentTimestamp_replacesSourcesWithoutCoalescing() {
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .processedTimestamp(MOVE_IN_TS)
                .moveInState(new MoveInState(MOVE_IN_TS, "00001", List.of("src1")))
                .build();

        TaskSyncContext result = new PublishMoveInStateOperation(
                "dst", OTHER_TS, "00002", List.of("src2"), false).doOperation(contextWith(dest));

        PartitionState updated = destPartition(result);

        assertEquals(List.of("src2"), updated.getMoveInState().getSourcePartitionTokens(),
                "src1 must not appear — different timestamps mean different MoveIn events");
        assertEquals(OTHER_TS, updated.getMoveInState().getTimestamp());
    }

    /**
     * If the same source token appears in both the already-stored {@code moveInState} and the
     * new call (e.g. because a MoveIn notification is replayed), the merged list must contain
     * it exactly once.
     */
    @Test
    void subsequentMoveIn_duplicateSource_isDeduplicatedInCoalescedList() {
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .processedTimestamp(MOVE_IN_TS)
                .moveInState(new MoveInState(MOVE_IN_TS, "00001", List.of("src1", "src2")))
                .build();

        // src2 appears in both the existing state and the new operation.
        TaskSyncContext result = new PublishMoveInStateOperation(
                "dst", MOVE_IN_TS, "00002", List.of("src2", "src3"), false).doOperation(contextWith(dest));

        PartitionState updated = destPartition(result);

        List<String> sources = updated.getMoveInState().getSourcePartitionTokens();
        assertEquals(3, sources.size(), "src2 must appear exactly once after deduplication");
        assertTrue(sources.containsAll(List.of("src1", "src2", "src3")));
    }

    /**
     * A subsequent MoveIn with no pre-existing {@code moveInState} (e.g. if the first call was
     * somehow skipped) must not coalesce — it simply creates a new state with the given tokens.
     */
    @Test
    void subsequentMoveIn_noExistingMoveInState_createsNewStateWithoutCoalescing() {
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .processedTimestamp(OLD_PROCESSED_TS)
                .build();

        TaskSyncContext result = new PublishMoveInStateOperation(
                "dst", MOVE_IN_TS, "00001", List.of("src1"), false).doOperation(contextWith(dest));

        PartitionState updated = destPartition(result);

        assertEquals(List.of("src1"), updated.getMoveInState().getSourcePartitionTokens());
        // processedTimestamp must not be pinned on a non-first MoveIn.
        assertEquals(OLD_PROCESSED_TS, updated.getProcessedTimestamp());
    }

    // -----------------------------------------------------------------------
    // Isolation
    // -----------------------------------------------------------------------

    /** Partitions whose token does not match the operation must be returned unchanged. */
    @Test
    void otherPartitionsAreUntouched() {
        PartitionState other = PartitionState.builder()
                .token("other")
                .state(PartitionStateEnum.RUNNING)
                .processedTimestamp(OLD_PROCESSED_TS)
                .build();
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .processedTimestamp(OLD_PROCESSED_TS)
                .build();

        TaskSyncContext context = TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(other, dest))
                        .sharedPartitions(List.of())
                        .build())
                .build();

        TaskSyncContext result = new PublishMoveInStateOperation(
                "dst", MOVE_IN_TS, "00001", List.of("src1"), true).doOperation(context);

        PartitionState otherAfter = result.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals("other"))
                .findFirst()
                .orElseThrow();

        assertEquals(OLD_PROCESSED_TS, otherAfter.getProcessedTimestamp());
        assertNull(otherAfter.getMoveInState());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static TaskSyncContext contextWith(PartitionState partition) {
        return TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(partition))
                        .sharedPartitions(List.of())
                        .build())
                .build();
    }

    private static PartitionState destPartition(TaskSyncContext ctx) {
        return ctx.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals("dst"))
                .findFirst()
                .orElseThrow();
    }
}
