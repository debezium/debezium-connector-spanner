/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Verifies that a finished source partition which recorded a {@link MoveOutState} is not
 * deleted (losing that state) until every destination it lists has itself streamed past the
 * move's commit timestamp - closing the race where a source is purged before a lagging
 * destination ever discovers the dependency and processes its MoveIn event.
 */
class RemoveFinishedPartitionOperationTest {

    private static final Timestamp FINISHED_LONG_AGO = Timestamp.ofTimeSecondsAndNanos(1, 0);
    private static final Timestamp BEFORE_MOVE = Timestamp.ofTimeSecondsAndNanos(500, 0);
    private static final Timestamp MOVE_TS = Timestamp.ofTimeSecondsAndNanos(1000, 0);
    private static final Timestamp AFTER_MOVE = Timestamp.ofTimeSecondsAndNanos(1500, 0);

    private RemoveFinishedPartitionOperation newOperation() {
        SpannerEventDispatcher spannerEventDispatcher = mock(SpannerEventDispatcher.class);
        SpannerConnectorConfig connectorConfig = mock(SpannerConnectorConfig.class);
        lenient().when(connectorConfig.getFinishedPartitionDeletionDelay()).thenReturn(Duration.ZERO);
        return new RemoveFinishedPartitionOperation(spannerEventDispatcher, connectorConfig);
    }

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

    private boolean isPresent(TaskSyncContext context, String token) {
        return context.getCurrentTaskState().getPartitions().stream()
                .anyMatch(p -> p.getToken().equals(token));
    }

    /**
     * This is the exact race from the review comment: "src" finished and moved part of its
     * range to "dst", but "dst" hasn't caught up to the move's commit timestamp in its own
     * stream yet - it hasn't even discovered the dependency (empty parents, no moveInState).
     * The pre-existing {@code allChildrenFinished} check alone would find zero children
     * referencing "src" and would wrongly allow deletion here; only the new
     * {@code moveOutDestinationsHaveResumed} check catches this.
     */
    @Test
    void destinationNotYetCaughtUpToTheMove_sourceNotDeleted() {
        PartitionState source = PartitionState.builder()
                .token("src")
                .state(PartitionStateEnum.FINISHED)
                .parents(Set.of())
                .finishedTimestamp(FINISHED_LONG_AGO)
                .moveOutStates(List.of(new MoveOutState(MOVE_TS, List.of("dst"))))
                .build();
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .processedTimestamp(BEFORE_MOVE)
                .build();
        TaskSyncContext context = contextWith(source, dest);

        TaskSyncContext result = newOperation().doOperation(context);

        assertTrue(isPresent(result, "src"), "source must not be deleted while its destination hasn't reached the move's timestamp yet");
    }

    @Test
    void destinationHasCaughtUpAndResumed_sourceDeleted() {
        // "dst" already streamed past the move's timestamp and resumed (moveInState cleared by
        // TakePartitionForStreamingOperation); its parents were set to ["src"] in the same step
        // that bumped processedTimestamp, so allChildrenFinished's own children check also
        // needs "dst" itself finished/removed for full deletion - reflect that here too.
        PartitionState source = PartitionState.builder()
                .token("src")
                .state(PartitionStateEnum.FINISHED)
                .parents(Set.of())
                .finishedTimestamp(FINISHED_LONG_AGO)
                .moveOutStates(List.of(new MoveOutState(MOVE_TS, List.of("dst"))))
                .build();
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.FINISHED)
                .parents(Set.of("src"))
                .processedTimestamp(AFTER_MOVE)
                .finishedTimestamp(FINISHED_LONG_AGO)
                .build();
        TaskSyncContext context = contextWith(source, dest);

        TaskSyncContext result = newOperation().doOperation(context);

        assertFalse(isPresent(result, "src"), "source must be deleted once its destination has passed the move and itself finished");
    }

    @Test
    void destinationHasCaughtUpButNotYetFinished_sourceNotDeletedByPreexistingLineageCheck() {
        // Once "dst" reaches the move's timestamp, MoveInStateUpdateOperation has already set
        // dst.parents = ["src"], so the pre-existing allChildrenFinished lineage check now keeps
        // "src" alive until "dst" itself finishes too - this is intentional, pre-existing
        // behavior unrelated to the new check, exercised here for completeness.
        PartitionState source = PartitionState.builder()
                .token("src")
                .state(PartitionStateEnum.FINISHED)
                .parents(Set.of())
                .finishedTimestamp(FINISHED_LONG_AGO)
                .moveOutStates(List.of(new MoveOutState(MOVE_TS, List.of("dst"))))
                .build();
        PartitionState dest = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of("src"))
                .processedTimestamp(AFTER_MOVE)
                .build();
        TaskSyncContext context = contextWith(source, dest);

        TaskSyncContext result = newOperation().doOperation(context);

        assertTrue(isPresent(result, "src"), "source must not be deleted while its resumed destination is still running");
    }

    @Test
    void destinationNotTrackedAnywhere_sourceDeleted() {
        // The destination token can't be found anywhere at all - nothing left depending on it.
        PartitionState source = PartitionState.builder()
                .token("src")
                .state(PartitionStateEnum.FINISHED)
                .parents(Set.of())
                .finishedTimestamp(FINISHED_LONG_AGO)
                .moveOutStates(List.of(new MoveOutState(MOVE_TS, List.of("dst"))))
                .build();
        TaskSyncContext context = contextWith(source);

        TaskSyncContext result = newOperation().doOperation(context);

        assertFalse(isPresent(result, "src"), "source must be deleted when its destination is nowhere to be found");
    }

    /**
     * A source never pauses for its own MoveOut events, so it can accumulate several independent
     * pending moves (to different destinations, at different timestamps) over its lifetime.
     * Every entry must individually be resolved before the source can be deleted - resolving
     * only the most recent one must not be enough.
     */
    @Test
    void multiplePendingMovesToDifferentDestinations_sourceNotDeletedUntilBothResumed() {
        PartitionState source = PartitionState.builder()
                .token("src")
                .state(PartitionStateEnum.FINISHED)
                .parents(Set.of())
                .finishedTimestamp(FINISHED_LONG_AGO)
                .moveOutStates(List.of(
                        new MoveOutState(MOVE_TS, List.of("dst1")),
                        new MoveOutState(AFTER_MOVE, List.of("dst2"))))
                .build();
        PartitionState dest1 = PartitionState.builder()
                .token("dst1")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .processedTimestamp(BEFORE_MOVE)
                .build();
        PartitionState dest2 = PartitionState.builder()
                .token("dst2")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .processedTimestamp(AFTER_MOVE)
                .build();
        TaskSyncContext context = contextWith(source, dest1, dest2);

        TaskSyncContext result = newOperation().doOperation(context);

        assertTrue(isPresent(result, "src"),
                "source must not be deleted while dst1's earlier move hasn't been resumed, even though dst2's later move has");
    }

    @Test
    void immutableKeyRangePartition_deletionUnaffectedByNewCheck() {
        // No moveOutState at all (the immutable key range path never sets one) - deletion
        // behaves exactly as it did before mutable key range support was introduced.
        PartitionState source = PartitionState.builder()
                .token("src")
                .state(PartitionStateEnum.FINISHED)
                .parents(Set.of())
                .finishedTimestamp(FINISHED_LONG_AGO)
                .build();
        TaskSyncContext context = contextWith(source);

        TaskSyncContext result = newOperation().doOperation(context);

        assertFalse(isPresent(result, "src"), "immutable-style finished partition with no moveOutState must still be deleted");
    }
}
