/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
 * Reproduces the exact scenario from the "Partition Move-in/Move-out Ordering"
 * section of the mutable key range design doc: a destination partition P0
 * receives three sequential MoveIn boundaries - from P1, then P2, then P3 -
 * all at the same commit timestamp TS0, and must cancel/pause/resume its
 * query according to each source's independently published MoveOutState.
 *
 * <p>Preconditions (per the design doc's "Assume" list): P1 has <b>not</b> yet
 * processed any MoveOut event. P2 <b>already</b> processed its MoveOut event at
 * TS0, destined for P0, before P0 ever saw the MoveIn. P3 <b>already</b>
 * processed a MoveOut event at TS0 destined for P0, and has since processed a
 * newer one at TS1 (TS1 &gt; TS0) destined for P4.
 *
 * <pre>
 * 1) P0 hits MoveIn(P1, TS0, seq "00000")           -&gt; P0 blocked, waiting for P1
 *    P1 publishes MoveOutState{TS0, [P0]}           -&gt; P0 resumes
 * 2) P0 restarts, skips seq &lt;= "00000", hits
 *    MoveIn(P2, TS0, seq "00002")                   -&gt; P2 already published
 *    MoveOutState{TS0, [P0]}                        -&gt; P0 continues immediately
 * 3) P0 restarts, skips seq &lt;= "00002", processes
 *    a normal data record (seq "00003"), then hits
 *    MoveIn(P3, TS0, seq "00005")                   -&gt; P3 already published
 *    MoveOutState{TS1, [P4]} with TS1 &gt; TS0         -&gt; P0 continues immediately
 * </pre>
 *
 * Final state matches the design doc table exactly:
 * <pre>
 * P0: MoveInState: {TS0, "00005"} Parent: P3
 * P1: MoveOutState: {TS0, [P0]}
 * P2: MoveOutState: {TS0, [P0]}
 * P3: MoveOutState: {TS1, [P4]}
 * </pre>
 */
class OrderedMoveInMoveOutScenarioTest {

    private static final Timestamp TS0 = Timestamp.ofTimeSecondsAndNanos(1000, 0);
    private static final Timestamp TS1 = Timestamp.ofTimeSecondsAndNanos(2000, 0);

    private TaskSyncContext context;

    private void seedPartitions(PartitionState... partitions) {
        context = TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(partitions))
                        .sharedPartitions(List.of())
                        .build())
                .build();
    }

    /** Mirrors {@code TaskStateChangeEventHandler.processEvent(MoveInNotificationEvent)}. */
    private void moveIn(String destToken, Timestamp ts, String recordSequence, String... sources) {
        context = new MoveInStateUpdateOperation(destToken, ts, recordSequence, List.of(sources)).doOperation(context);
        context = new FindPartitionForStreamingOperation().doOperation(context);
    }

    /** Mirrors {@code TaskStateChangeEventHandler.processEvent(MoveOutNotificationEvent)}. */
    private void moveOut(String sourceToken, Timestamp ts, String... destinations) {
        context = new MoveOutStateUpdateOperation(sourceToken, ts, List.of(destinations)).doOperation(context);
    }

    /** Mirrors the periodic {@code processSyncEvent()} re-evaluation on the destination's task. */
    private void refresh() {
        context = new FindPartitionForStreamingOperation().doOperation(context);
    }

    private PartitionState partition(String token) {
        return context.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals(token))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void sequentialMoveInsAtSameTimestampFromDifferentSources() {
        // Initial: P0 is RUNNING. P1 has not moved out yet. P2 already moved out to P0
        // at TS0. P3 already moved out to P0 at TS0, then moved out again to P4 at a
        // later timestamp TS1 - all per the design doc's stated preconditions.
        seedPartitions(
                PartitionState.builder().token("P0").state(PartitionStateEnum.RUNNING).parents(Set.of()).build(),
                PartitionState.builder().token("P1").state(PartitionStateEnum.RUNNING).parents(Set.of()).build(),
                PartitionState.builder().token("P2").state(PartitionStateEnum.RUNNING).parents(Set.of())
                        .moveOutStates(List.of(new MoveOutState(TS0, List.of("P0"))))
                        .build(),
                PartitionState.builder().token("P3").state(PartitionStateEnum.RUNNING).parents(Set.of())
                        .moveOutStates(List.of(
                                new MoveOutState(TS0, List.of("P0")),
                                new MoveOutState(TS1, List.of("P4"))))
                        .build());

        // --- 1st record: MoveIn from P1 at TS0, seq "00000" ---
        moveIn("P0", TS0, "00000", "P1");
        assertEquals(PartitionStateEnum.CREATED, partition("P0").getState(),
                "P0 must pause: P1 has not published MoveOutState yet");
        assertEquals(Set.of("P1"), partition("P0").getParents());
        assertEquals(TS0, partition("P0").getMoveInState().getTimestamp());
        assertEquals("00000", partition("P0").getMoveInState().getRecordSequence());

        // P1 processes its own MoveOut event at TS0, destined for P0.
        moveOut("P1", TS0, "P0");
        refresh();
        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partition("P0").getState(),
                "P0 must resume: P1 moved out at the same TS0 and lists P0 as destination");

        // --- P0 restarts the query, skips seq <= "00000", hits the 2nd record: ---
        // MoveIn from P2 at TS0, seq "00002".
        moveIn("P0", TS0, "00002", "P2");
        assertEquals(Set.of("P2"), partition("P0").getParents());
        assertEquals("00002", partition("P0").getMoveInState().getRecordSequence());
        // P2 already published its MoveOutState{TS0, [P0]} before P0 ever saw this MoveIn
        // (a seeded precondition, not a subsequent event), so P0 continues immediately
        // without ever genuinely blocking - no separate moveOut()/refresh() needed.
        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partition("P0").getState(),
                "P0 must continue immediately: P2 already moved out to P0 at the same TS0");

        // --- P0 restarts, skips seq <= "00002", processes the 3rd record (a normal data
        // record, seq "00003") normally, then hits the 4th record: MoveIn from P3 at TS0,
        // seq "00005".
        moveIn("P0", TS0, "00005", "P3");
        assertEquals(Set.of("P3"), partition("P0").getParents());
        assertEquals("00005", partition("P0").getMoveInState().getRecordSequence());
        // P3 already published a *later* MoveOut (TS1 > TS0), so P0 continues immediately
        // without waiting - matching the doc's final step.
        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partition("P0").getState(),
                "P0 must continue immediately: P3's MoveOutState timestamp TS1 is already past TS0");

        // Final state matches the design doc table exactly (P3 additionally retains its
        // earlier TS0 move to P0, since a source never overwrites - only accumulates - its
        // MoveOut history).
        assertEquals(1, partition("P1").getMoveOutStates().size());
        assertEquals(TS0, partition("P1").getMoveOutStates().get(0).getTimestamp());
        assertEquals(List.of("P0"), partition("P1").getMoveOutStates().get(0).getDestPartitionTokens());
        assertEquals(1, partition("P2").getMoveOutStates().size());
        assertEquals(TS0, partition("P2").getMoveOutStates().get(0).getTimestamp());
        assertEquals(List.of("P0"), partition("P2").getMoveOutStates().get(0).getDestPartitionTokens());
        assertEquals(2, partition("P3").getMoveOutStates().size());
        assertEquals(TS1, partition("P3").getMoveOutStates().get(1).getTimestamp());
        assertEquals(List.of("P4"), partition("P3").getMoveOutStates().get(1).getDestPartitionTokens());
    }
}
