/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Verifies that {@link MoveOutStateUpdateOperation} merges new MoveOut state with
 * existing entries: each destination token is kept at the latest timestamp it has been
 * associated with, while independent moves to different destinations are preserved and
 * the resulting list is sorted by ascending commit timestamp.
 */
class MoveOutStateUpdateOperationTest {

    private static final Timestamp T0 = Timestamp.ofTimeSecondsAndNanos(100, 0);
    private static final Timestamp T1 = Timestamp.ofTimeSecondsAndNanos(200, 0);
    private static final Timestamp T2 = Timestamp.ofTimeSecondsAndNanos(300, 0);

    private TaskSyncContext contextWith(PartitionState... partitions) {
        return TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(partitions))
                        .sharedPartitions(List.of())
                        .build())
                .build();
    }

    private List<MoveOutState> moveOutStatesOf(TaskSyncContext context, String token) {
        return context.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals(token))
                .findFirst()
                .orElseThrow()
                .getMoveOutStates();
    }

    @Test
    void firstMoveOutStateIsRecorded() {
        PartitionState source = sourcePartition("src", List.of());
        TaskSyncContext context = contextWith(source);

        TaskSyncContext result = new MoveOutStateUpdateOperation("src", T1, List.of("dst1"))
                .doOperation(context);

        assertEquals(List.of(new MoveOutState(T1, List.of("dst1"))),
                moveOutStatesOf(result, "src"));
    }

    @Test
    void independentMovesToDifferentDestinationsAtDifferentTimestampsArePreserved() {
        PartitionState source = sourcePartition("src", List.of(
                new MoveOutState(T1, List.of("dst1"))));
        TaskSyncContext context = contextWith(source);

        TaskSyncContext result = new MoveOutStateUpdateOperation("src", T2, List.of("dst2"))
                .doOperation(context);

        assertEquals(List.of(
                new MoveOutState(T1, List.of("dst1")),
                new MoveOutState(T2, List.of("dst2"))),
                moveOutStatesOf(result, "src"));
    }

    @Test
    void sameDestinationAtNewerTimestampReplacesOlderEntry() {
        PartitionState source = sourcePartition("src", List.of(
                new MoveOutState(T1, List.of("dst1"))));
        TaskSyncContext context = contextWith(source);

        TaskSyncContext result = new MoveOutStateUpdateOperation("src", T2, List.of("dst1"))
                .doOperation(context);

        assertEquals(List.of(new MoveOutState(T2, List.of("dst1"))),
                moveOutStatesOf(result, "src"));
    }

    @Test
    void sameDestinationAtOlderTimestampIsIgnored() {
        PartitionState source = sourcePartition("src", List.of(
                new MoveOutState(T2, List.of("dst1"))));
        TaskSyncContext context = contextWith(source);

        TaskSyncContext result = new MoveOutStateUpdateOperation("src", T1, List.of("dst1"))
                .doOperation(context);

        assertEquals(List.of(new MoveOutState(T2, List.of("dst1"))),
                moveOutStatesOf(result, "src"));
    }

    @Test
    void sameTimestampMergesDestinationTokens() {
        PartitionState source = sourcePartition("src", List.of(
                new MoveOutState(T1, List.of("dst1"))));
        TaskSyncContext context = contextWith(source);

        TaskSyncContext result = new MoveOutStateUpdateOperation("src", T1, List.of("dst1", "dst2"))
                .doOperation(context);

        assertEquals(List.of(new MoveOutState(T1, List.of("dst1", "dst2"))),
                moveOutStatesOf(result, "src"));
    }

    @Test
    void resultIsSortedByAscendingTimestamp() {
        PartitionState source = sourcePartition("src", List.of(
                new MoveOutState(T2, List.of("dst2"))));
        TaskSyncContext context = contextWith(source);

        TaskSyncContext result = new MoveOutStateUpdateOperation("src", T0, List.of("dst0"))
                .doOperation(context);

        assertEquals(List.of(
                new MoveOutState(T0, List.of("dst0")),
                new MoveOutState(T2, List.of("dst2"))),
                moveOutStatesOf(result, "src"));
    }

    @Test
    void moveOutForDifferentDestinationDoesNotAlterOtherTimestamps() {
        PartitionState source = sourcePartition("src", List.of(
                new MoveOutState(T1, List.of("dst1")),
                new MoveOutState(T2, List.of("dst2"))));
        TaskSyncContext context = contextWith(source);

        // dst1 advances to T3, dst2 should stay at T2.
        Timestamp T3 = Timestamp.ofTimeSecondsAndNanos(400, 0);
        TaskSyncContext result = new MoveOutStateUpdateOperation("src", T3, List.of("dst1"))
                .doOperation(context);

        assertEquals(List.of(
                new MoveOutState(T2, List.of("dst2")),
                new MoveOutState(T3, List.of("dst1"))),
                moveOutStatesOf(result, "src"));
    }

    @Test
    void missingPartitionIsNotCreated() {
        PartitionState other = sourcePartition("other", List.of());
        TaskSyncContext context = contextWith(other);

        TaskSyncContext result = new MoveOutStateUpdateOperation("missing", T1, List.of("dst1"))
                .doOperation(context);

        Set<String> tokens = result.getCurrentTaskState().getPartitions().stream()
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
        assertEquals(Set.of("other"), tokens);
    }

    @Test
    void mergeMoveOutStates_emptyExisting_returnsIncoming() {
        assertEquals(List.of(new MoveOutState(T1, List.of("dst1"))),
                MoveOutStateUpdateOperation.mergeMoveOutStates(List.of(), new MoveOutState(T1, List.of("dst1"))));
    }

    @Test
    void mergeMoveOutStates_sameTimestampMergesDestinations() {
        List<MoveOutState> existing = List.of(new MoveOutState(T1, List.of("dst1")));
        MoveOutState incoming = new MoveOutState(T1, List.of("dst1", "dst2"));

        assertEquals(List.of(new MoveOutState(T1, List.of("dst1", "dst2"))),
                MoveOutStateUpdateOperation.mergeMoveOutStates(existing, incoming));
    }

    @Test
    void mergeMoveOutStates_newerTimestampForSameDestinationReplacesOlder() {
        List<MoveOutState> existing = List.of(new MoveOutState(T1, List.of("dst1")));
        MoveOutState incoming = new MoveOutState(T2, List.of("dst1"));

        assertEquals(List.of(new MoveOutState(T2, List.of("dst1"))),
                MoveOutStateUpdateOperation.mergeMoveOutStates(existing, incoming));
    }

    @Test
    void mergeMoveOutStates_olderTimestampForSameDestinationIsIgnored() {
        List<MoveOutState> existing = List.of(new MoveOutState(T2, List.of("dst1")));
        MoveOutState incoming = new MoveOutState(T1, List.of("dst1"));

        assertEquals(List.of(new MoveOutState(T2, List.of("dst1"))),
                MoveOutStateUpdateOperation.mergeMoveOutStates(existing, incoming));
    }

    @Test
    void mergeMoveOutStates_independentDestinationsAtDifferentTimestampsArePreservedAndSorted() {
        List<MoveOutState> existing = List.of(new MoveOutState(T1, List.of("dst1")));
        MoveOutState incoming = new MoveOutState(T2, List.of("dst2"));

        assertEquals(List.of(
                new MoveOutState(T1, List.of("dst1")),
                new MoveOutState(T2, List.of("dst2"))),
                MoveOutStateUpdateOperation.mergeMoveOutStates(existing, incoming));
    }

    @Test
    void mergeMoveOutStates_oneDestinationAdvancingDoesNotAffectAnother() {
        List<MoveOutState> existing = List.of(
                new MoveOutState(T1, List.of("dst1")),
                new MoveOutState(T2, List.of("dst2")));
        Timestamp T3 = Timestamp.ofTimeSecondsAndNanos(400, 0);
        MoveOutState incoming = new MoveOutState(T3, List.of("dst1"));

        assertEquals(List.of(
                new MoveOutState(T2, List.of("dst2")),
                new MoveOutState(T3, List.of("dst1"))),
                MoveOutStateUpdateOperation.mergeMoveOutStates(existing, incoming));
    }

    private static PartitionState sourcePartition(String token, List<MoveOutState> moveOutStates) {
        return PartitionState.builder()
                .token(token)
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveOutStates(moveOutStates)
                .build();
    }
}
