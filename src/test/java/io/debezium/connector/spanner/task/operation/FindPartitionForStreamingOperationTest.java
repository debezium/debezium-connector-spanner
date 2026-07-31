/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveInState;
import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Verifies the {@code CanDestPartitionContinue} logic that decides when a destination
 * partition paused after a MoveIn event is allowed to resume streaming, per the
 * mutable key range design doc.
 */
class FindPartitionForStreamingOperationTest {

    private static final Timestamp MOVE_IN_TS = Timestamp.ofTimeSecondsAndNanos(1000, 0);
    private static final Timestamp BEFORE_MOVE_IN_TS = Timestamp.ofTimeSecondsAndNanos(500, 0);
    private static final Timestamp AFTER_MOVE_IN_TS = Timestamp.ofTimeSecondsAndNanos(1500, 0);

    private PartitionState destPartition(String destToken, String... sourceTokens) {
        return PartitionState.builder()
                .token(destToken)
                .state(PartitionStateEnum.CREATED)
                .parents(Set.of(sourceTokens))
                .moveInState(new MoveInState(MOVE_IN_TS, "00001", List.of(sourceTokens)))
                .build();
    }

    private TaskSyncContext contextWith(PartitionState dest, PartitionState... otherOwnedPartitions) {
        return TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(prepend(dest, otherOwnedPartitions))
                        .sharedPartitions(List.of())
                        .build())
                .build();
    }

    private TaskSyncContext contextWithShared(PartitionState dest, PartitionState sharedSource) {
        return TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(dest))
                        .sharedPartitions(List.of(sharedSource))
                        .build())
                .build();
    }

    private TaskSyncContext contextWithOtherTask(PartitionState dest, PartitionState sourceOnOtherTask) {
        return TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(dest))
                        .sharedPartitions(List.of())
                        .build())
                .taskStates(Map.of("task1", TaskState.builder()
                        .taskUid("task1")
                        .partitions(List.of(sourceOnOtherTask))
                        .sharedPartitions(List.of())
                        .build()))
                .build();
    }

    private List<PartitionState> prepend(PartitionState first, PartitionState... rest) {
        java.util.ArrayList<PartitionState> list = new java.util.ArrayList<>();
        list.add(first);
        list.addAll(List.of(rest));
        return list;
    }

    @Test
    void sourceNotYetSeen_destPartitionBlocked() {
        PartitionState dest = destPartition("dst", "src1");
        TaskSyncContext context = contextWith(dest);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.CREATED, partitionState(result, "dst").getState());
    }

    @Test
    void sourceMoveOutBeforeMoveInTimestamp_destPartitionBlocked() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(List.of(new MoveOutState(BEFORE_MOVE_IN_TS, List.of("dst"))))
                .build();
        TaskSyncContext context = contextWith(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.CREATED, partitionState(result, "dst").getState());
    }

    @Test
    void sourceMoveOutAtSameTimestampWithoutDestToken_destPartitionBlocked() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(List.of(new MoveOutState(MOVE_IN_TS, List.of("someOtherDest"))))
                .build();
        TaskSyncContext context = contextWith(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.CREATED, partitionState(result, "dst").getState());
    }

    @Test
    void sourceMoveOutAtSameTimestampWithDestToken_destPartitionReady() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(List.of(new MoveOutState(MOVE_IN_TS, List.of("dst"))))
                .build();
        TaskSyncContext context = contextWith(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partitionState(result, "dst").getState());
    }

    @Test
    void sourceMoveOutAfterMoveInTimestamp_destPartitionReady() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(List.of(new MoveOutState(AFTER_MOVE_IN_TS, List.of("someOtherDest"))))
                .build();
        TaskSyncContext context = contextWith(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partitionState(result, "dst").getState());
    }

    @Test
    void multipleSources_allMustBeReadyBeforeDestPartitionContinues() {
        PartitionState dest = destPartition("dst", "src1", "src2");
        PartitionState source1 = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(List.of(new MoveOutState(MOVE_IN_TS, List.of("dst"))))
                .build();
        PartitionState source2 = PartitionState.builder()
                .token("src2")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(List.of(new MoveOutState(BEFORE_MOVE_IN_TS, List.of("dst"))))
                .build();
        TaskSyncContext context = contextWith(dest, source1, source2);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.CREATED, partitionState(result, "dst").getState());
    }

    @Test
    void sourceFoundInSharedPartitions_destPartitionReady() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(List.of(new MoveOutState(MOVE_IN_TS, List.of("dst"))))
                .build();
        TaskSyncContext context = contextWithShared(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partitionState(result, "dst").getState());
    }

    @Test
    void sourceFoundOnOtherTask_destPartitionReady() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(List.of(new MoveOutState(MOVE_IN_TS, List.of("dst"))))
                .build();
        TaskSyncContext context = contextWithOtherTask(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partitionState(result, "dst").getState());
    }

    @Test
    void sourceFinishedAndPurged_destPartitionReady() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState finishedSource = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.FINISHED)
                .parents(Set.of())
                .finishedTimestamp(AFTER_MOVE_IN_TS)
                .build();
        TaskSyncContext context = contextWith(dest, finishedSource);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partitionState(result, "dst").getState());
    }

    /**
     * Regression test for review comment 7: a task crashed after the source's own change stream
     * had already read past the MoveIn timestamp, but before MoveOutStateUpdateOperation's
     * update was persisted to the sync topic. On restart the source resumes from its persisted
     * processedTimestamp (already past the MoveIn timestamp) and will never re-emit that
     * boundary, so moveOutState stays null forever - but processedTimestamp alone proves the
     * source already streamed past the point in question, so the destination must not deadlock.
     */
    @Test
    void sourceMissingMoveOutButAlreadyStreamedPastMoveInTimestamp_destPartitionReady() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .processedTimestamp(AFTER_MOVE_IN_TS)
                .build();
        TaskSyncContext context = contextWith(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partitionState(result, "dst").getState());
    }

    @Test
    void sourceMissingMoveOutAndNotYetPastMoveInTimestamp_destPartitionStillBlocked() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .processedTimestamp(BEFORE_MOVE_IN_TS)
                .build();
        TaskSyncContext context = contextWith(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.CREATED, partitionState(result, "dst").getState(),
                "source hasn't actually reached the MoveIn timestamp yet - must still block");
    }

    @Test
    void sourceMissingMoveOutAndExactlyAtMoveInTimestamp_destPartitionStillBlocked() {
        // Strictly greater-than is required: at the exact timestamp the source may not have
        // fully processed every record at that instant yet.
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .processedTimestamp(MOVE_IN_TS)
                .build();
        TaskSyncContext context = contextWith(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.CREATED, partitionState(result, "dst").getState());
    }

    /**
     * A source can accumulate several independent MoveOutState entries over its life (it never
     * pauses for its own MoveOut events). An older, unrelated entry (to a different destination,
     * at an earlier timestamp) must not mask the crash-recovery fallback for a *different*,
     * later MoveOut whose record was lost before being persisted.
     */
    @Test
    void olderUnrelatedMoveOutStateDoesNotMaskCrashRecoveryForLaterMove() {
        PartitionState dest = destPartition("dst", "src1");
        PartitionState source = PartitionState.builder()
                .token("src1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(List.of(new MoveOutState(BEFORE_MOVE_IN_TS, List.of("someOtherDest"))))
                .processedTimestamp(AFTER_MOVE_IN_TS)
                .build();
        TaskSyncContext context = contextWith(dest, source);

        TaskSyncContext result = new FindPartitionForStreamingOperation().doOperation(context);

        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partitionState(result, "dst").getState(),
                "an older, unrelated MoveOutState entry must not block the crash-recovery fallback for this later move");
    }

    private PartitionState partitionState(TaskSyncContext context, String token) {
        return context.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals(token))
                .findFirst()
                .orElseThrow();
    }
}
