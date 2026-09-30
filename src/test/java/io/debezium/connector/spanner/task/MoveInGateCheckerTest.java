/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.PartitionKey;
import io.debezium.connector.spanner.kafka.internal.model.MoveInState;
import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;

/**
 * Verifies that {@link MoveInGateChecker} scopes all partition, MoveOut state and finished
 * lookups by TVF name. A source/destination pair belongs to a single TVF; partitions in
 * other TVFs with the same raw token must not influence the gate decision.
 */
class MoveInGateCheckerTest {

    private static final Timestamp T1 = Timestamp.ofTimeSecondsAndNanos(100, 0);
    private static final Timestamp T2 = Timestamp.ofTimeSecondsAndNanos(200, 0);

    @Test
    void findPartitionState_sameTokenDifferentTvf_returnsNull() {
        PartitionState partitionTvfA = partition("src", "tvfA", PartitionStateEnum.RUNNING);
        TaskSyncContext ctx = contextWithCurrent(partitionTvfA);

        assertNotNull(MoveInGateChecker.findPartitionState(ctx, "src", "tvfA"));
        assertNull(MoveInGateChecker.findPartitionState(ctx, "src", "tvfB"),
                "must not match a partition with the same token in a different TVF");
        assertNull(MoveInGateChecker.findPartitionState(ctx, "src", null),
                "must not match a per-TVF partition when queried with legacy null TVF");
    }

    @Test
    void findPartitionState_legacyNullTvfMatchesNullTvfPartition() {
        PartitionState legacyPartition = partition("src", null, PartitionStateEnum.RUNNING);
        TaskSyncContext ctx = contextWithCurrent(legacyPartition);

        assertNotNull(MoveInGateChecker.findPartitionState(ctx, "src", null));
        assertNull(MoveInGateChecker.findPartitionState(ctx, "src", "tvfA"),
                "legacy null-tvf partition must not satisfy a per-TVF lookup");
    }

    @Test
    void getFinishedPartitions_returnsTvfAwareIdentities() {
        PartitionState finishedA = partition("p", "tvfA", PartitionStateEnum.FINISHED);
        PartitionState finishedB = partition("p", "tvfB", PartitionStateEnum.FINISHED);
        PartitionState running = partition("p", null, PartitionStateEnum.RUNNING);

        TaskSyncContext ctx = contextWithCurrent(finishedA, finishedB, running);

        Set<PartitionKey> finished = MoveInGateChecker.getFinishedPartitions(ctx);

        assertEquals(Set.of(new PartitionKey("p", "tvfA"), new PartitionKey("p", "tvfB")), finished,
                "finished set must contain TVF-scoped identities, not raw tokens");
    }

    @Test
    void canContinue_sourceFinishedInSameTvf_returnsTrue() {
        PartitionState source = partition("src", "tvfA", PartitionStateEnum.FINISHED);
        TaskSyncContext ctx = contextWithCurrent(source);

        Set<PartitionKey> finished = MoveInGateChecker.getFinishedPartitions(ctx);

        assertTrue(MoveInGateChecker.canContinue(ctx, "dst", "tvfA", T1, List.of("src"), finished),
                "source finished in the destination's TVF must satisfy the gate");
    }

    @Test
    void canContinue_sourceFinishedInCoLocatedTvf_returnsTrue() {
        PartitionState sourceOtherTvf = partition("src", "tvfB", PartitionStateEnum.FINISHED);
        TaskSyncContext ctx = contextWithCurrent(sourceOtherTvf);

        Set<PartitionKey> finished = MoveInGateChecker.getFinishedPartitions(ctx);

        assertTrue(MoveInGateChecker.canContinue(ctx, "dst", "tvfA", T1, List.of("src"), finished),
                "cross-placement source finished in co-located tvfB must satisfy the gate for destination in tvfA");
    }

    @Test
    void canContinue_moveOutStateScopedByTvf() {
        PartitionState sourceTvfA = partition("src", "tvfA", PartitionStateEnum.RUNNING,
                List.of(new MoveOutState(T1, List.of("dst"))));
        PartitionState sourceTvfB = partition("src", "tvfB", PartitionStateEnum.RUNNING,
                List.of(new MoveOutState(T1, List.of("dst"))));
        TaskSyncContext ctx = contextWithCurrent(sourceTvfA, sourceTvfB);

        Set<PartitionKey> finished = MoveInGateChecker.getFinishedPartitions(ctx);

        assertTrue(MoveInGateChecker.canContinue(ctx, "dst", "tvfA", T1, List.of("src"), finished),
                "MoveOut state from the same TVF must satisfy the gate");
        assertFalse(MoveInGateChecker.canContinue(ctx, "dst", "tvfA", T2, List.of("src"), finished),
                "MoveOut state at T1 must not satisfy a MoveIn at a later timestamp T2");
    }

    @Test
    void canContinue_crossTvfMoveOutStateInCoLocatedTvf_returnsTrueWithoutProbing() {
        PartitionState sourceTvfB = partition("src", "tvfB", PartitionStateEnum.RUNNING,
                List.of(new MoveOutState(T1, List.of("dst"))));
        TaskSyncContext ctx = contextWithCurrent(sourceTvfB);

        Set<PartitionKey> finished = MoveInGateChecker.getFinishedPartitions(ctx);

        MoveInGateChecker.PlacementTokenProbe probeShouldNotBeCalled = (token, ts) -> {
            throw new AssertionError("Probe must not be invoked when source is found in co-located TaskSyncContext");
        };
        assertTrue(MoveInGateChecker.canContinue(ctx, "dst", "tvfA", T1, List.of("src"), finished, probeShouldNotBeCalled),
                "MoveOut state from co-located tvfB must satisfy the gate for destination in tvfA without probing");
        assertFalse(MoveInGateChecker.canContinue(ctx, "dst", "tvfA", T2, List.of("src"), finished, probeShouldNotBeCalled),
                "Co-located cross-TVF source in tvfB that has not yet reached T2 must block without probing");
    }

    @Test
    void canContinue_externalPlacementSourceRejectedByProbe_returnsTrue() {
        TaskSyncContext ctx = contextWithCurrent();
        Set<PartitionKey> finished = MoveInGateChecker.getFinishedPartitions(ctx);

        assertTrue(MoveInGateChecker.canContinue(ctx, "dst", "tvfA", T1, List.of("ext-src"), finished,
                (token, ts) -> "ext-src".equals(token) && T1.equals(ts)),
                "external placement source confirmed by probe must satisfy the gate");
        assertFalse(MoveInGateChecker.canContinue(ctx, "dst", "tvfA", T1, List.of("same-placement-unregistered"), finished,
                (token, ts) -> false),
                "unregistered source accepted by a configured TVF must remain blocked until MoveOut arrives");
    }

    @Test
    void shouldAdvanceProcessedTimestampForMoveIn_advancesUntilMoveInSatisfied() {
        Timestamp tMoveIn = Timestamp.ofTimeSecondsAndNanos(150, 0);
        Timestamp tAfter = Timestamp.ofTimeSecondsAndNanos(160, 0);
        PartitionState source = partition("src", "tvfB", PartitionStateEnum.RUNNING);
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .tvfName("tvfA")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .moveInState(new MoveInState(tMoveIn, "0001", List.of("src")))
                .startTimestamp(T1)
                .build();
        TaskSyncContext ctx = contextWithCurrent(source, dest);

        assertTrue(MoveInGateChecker.shouldAdvanceProcessedTimestampForMoveIn(
                ctx, "src", "tvfB", tAfter, T1),
                "source with unresolved MoveInState and lastPublishedProcessedTs <= moveInTs should advance");
        assertFalse(MoveInGateChecker.shouldAdvanceProcessedTimestampForMoveIn(
                ctx, "src", "tvfB", tAfter, tAfter),
                "once lastPublishedProcessedTs > moveInTs, should not advance again");
    }

    private static TaskSyncContext contextWithCurrent(PartitionState... partitions) {
        return TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(partitions))
                        .sharedPartitions(List.of())
                        .build())
                .taskStates(Map.of())
                .build();
    }

    private static PartitionState partition(String token, String tvfName, PartitionStateEnum state) {
        return partition(token, tvfName, state, List.of());
    }

    private static PartitionState partition(String token, String tvfName, PartitionStateEnum state,
                                            List<MoveOutState> moveOutStates) {
        return PartitionState.builder()
                .token(token)
                .tvfName(tvfName)
                .state(state)
                .parents(Set.of())
                .moveOutStates(moveOutStates)
                .startTimestamp(T1)
                .build();
    }
}
