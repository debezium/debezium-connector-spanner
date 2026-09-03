/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.dao.ChangeStreamResultSetMetadata;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEventEvent;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Unit tests for {@link MoveInBufferGate} segmented drain behaviour.
 */
class MoveInBufferGateTest {

    private static final String DEST = "dest-token";
    private static final int MAX_EVENTS = 1000;

    private static final Timestamp T1 = Timestamp.ofTimeSecondsAndNanos(1_000, 0);
    private static final Timestamp T2 = Timestamp.ofTimeSecondsAndNanos(2_000, 0);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Builds a {@link TaskSyncContext} snapshot in which the given source tokens
     * are in {@code FINISHED} state (satisfying {@code MoveInGateChecker.canContinue}).
     */
    private static TaskSyncContext ctxWithFinished(String... finishedTokens) {
        List<PartitionState> partitions = Arrays.stream(finishedTokens)
                .map(token -> PartitionState.builder()
                        .token(token)
                        .state(PartitionStateEnum.FINISHED)
                        .build())
                .collect(Collectors.toList());
        return buildCtx(partitions);
    }

    /** Builds a {@link TaskSyncContext} snapshot where no source has confirmed. */
    private static TaskSyncContext ctxEmpty() {
        return buildCtx(List.of());
    }

    private static TaskSyncContext buildCtx(Collection<PartitionState> partitions) {
        TaskState taskState = mock(TaskState.class);
        when(taskState.getPartitions()).thenReturn(partitions);
        when(taskState.getSharedPartitions()).thenReturn(List.of());

        TaskSyncContext ctx = mock(TaskSyncContext.class);
        when(ctx.getCurrentTaskState()).thenReturn(taskState);
        when(ctx.getTaskStates()).thenReturn(Map.of());
        return ctx;
    }

    private static PartitionEventEvent moveInEvent() {
        return mock(PartitionEventEvent.class);
    }

    private static ChangeStreamEvent dataEvent() {
        return mock(ChangeStreamEvent.class);
    }

    private static ChangeStreamResultSetMetadata fakeMeta() {
        return mock(ChangeStreamResultSetMetadata.class);
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /**
     * Core segmentation test: with two segments where only the first source has
     * confirmed, {@code drainConfirmedPrefix()} releases Segment 1 in full
     * (MoveIn header + data events) and leaves Segment 2 untouched.
     */
    @Test
    void drainConfirmedPrefix_releasesOnlyConfirmedPrefixSegment() {
        // Context where only src1 is confirmed
        AtomicReference<TaskSyncContext> ctxRef = new AtomicReference<>(ctxEmpty());
        MoveInBufferGate gate = new MoveInBufferGate(DEST, MAX_EVENTS, ctxRef::get);

        PartitionEventEvent mi1 = moveInEvent();
        PartitionEventEvent mi2 = moveInEvent();
        ChangeStreamEvent d1 = dataEvent(), d2 = dataEvent();
        ChangeStreamEvent d3 = dataEvent(), d4 = dataEvent();

        gate.addMoveIn(T1, List.of("src1"), mi1, fakeMeta()); // Segment 1
        gate.addDataEvent(d1);
        gate.addDataEvent(d2);
        gate.addMoveIn(T2, List.of("src2"), mi2, fakeMeta()); // Segment 2
        gate.addDataEvent(d3);
        gate.addDataEvent(d4);

        assertEquals(6, gate.size()); // 2 MoveIn headers + 4 data events
        assertFalse(gate.isEmpty());

        // Before any confirmation: nothing released
        assertTrue(gate.drainConfirmedPrefix().isEmpty());

        // Confirm src1
        ctxRef.set(ctxWithFinished("src1"));
        List<ChangeStreamEvent> ready = gate.drainConfirmedPrefix();

        // Segment 1 released: MoveIn1 first, then d1, d2 — exact order
        assertEquals(List.of(mi1, d1, d2), ready);

        // Segment 2 still held
        assertFalse(gate.isEmpty());
        assertEquals(3, gate.size()); // 1 MoveIn header + 2 data events

        // Now confirm src2 too
        ctxRef.set(ctxWithFinished("src1", "src2"));
        List<ChangeStreamEvent> rest = gate.drainConfirmedPrefix();
        assertEquals(List.of(mi2, d3, d4), rest);
        assertTrue(gate.isEmpty());
    }

    /**
     * When a new MoveIn arrives while the previous one is still unresolved, data events
     * arriving before the new MoveIn stay in Segment 1 and are released only when
     * Segment 1's source confirms — independently of Segment 2.
     */
    @Test
    void newMoveInWhileResolvingPrevious_doesNotBlockEarlierSegment() {
        AtomicReference<TaskSyncContext> ctxRef = new AtomicReference<>(ctxEmpty());
        MoveInBufferGate gate = new MoveInBufferGate(DEST, MAX_EVENTS, ctxRef::get);

        PartitionEventEvent mi1 = moveInEvent();
        PartitionEventEvent mi2 = moveInEvent();
        ChangeStreamEvent before = dataEvent(); // arrives between MI1 and MI2
        ChangeStreamEvent after = dataEvent(); // arrives after MI2

        gate.addMoveIn(T1, List.of("src1"), mi1, fakeMeta());
        gate.addDataEvent(before);
        // MoveIn2 arrives while src1 is still unresolved
        gate.addMoveIn(T2, List.of("src2"), mi2, fakeMeta());
        gate.addDataEvent(after);

        // Nothing confirmed yet
        assertTrue(gate.drainConfirmedPrefix().isEmpty());

        // Confirm src1 only — Segment 1 (MI1 + before) must be released
        ctxRef.set(ctxWithFinished("src1"));
        List<ChangeStreamEvent> seg1 = gate.drainConfirmedPrefix();
        assertEquals(List.of(mi1, before), seg1);

        // Segment 2 still held (src2 not confirmed)
        assertFalse(gate.isEmpty());

        // Confirm src2 — Segment 2 (MI2 + after) released
        ctxRef.set(ctxWithFinished("src1", "src2"));
        List<ChangeStreamEvent> seg2 = gate.drainConfirmedPrefix();
        assertEquals(List.of(mi2, after), seg2);
        assertTrue(gate.isEmpty());
    }

    /**
     * No-data scenario (hot-split load test): 100 MoveIn events with no data events
     * between them.  Each segment's source confirming immediately releases only that
     * segment's MoveIn header, not all remaining segments.
     */
    @Test
    void noDataScenario_eachSegmentReleasedIndependentlyWithoutDelay() {
        // src-0 confirmed, others not
        AtomicReference<TaskSyncContext> ctxRef = new AtomicReference<>(ctxWithFinished("src-0"));
        MoveInBufferGate gate = new MoveInBufferGate(DEST, MAX_EVENTS, ctxRef::get);

        int n = 10;
        PartitionEventEvent[] mis = new PartitionEventEvent[n];
        for (int i = 0; i < n; i++) {
            mis[i] = moveInEvent();
            gate.addMoveIn(Timestamp.ofTimeSecondsAndNanos(i, 0), List.of("src-" + i), mis[i], fakeMeta());
        }

        assertEquals(n, gate.size());

        // Only src-0 confirmed → only segment 0 released
        List<ChangeStreamEvent> first = gate.drainConfirmedPrefix();
        assertEquals(List.of(mis[0]), first);
        assertEquals(n - 1, gate.size());

        // Confirm all remaining sources one by one
        for (int i = 1; i < n; i++) {
            String[] confirmedSoFar = new String[i + 1];
            for (int j = 0; j <= i; j++) {
                confirmedSoFar[j] = "src-" + j;
            }
            ctxRef.set(ctxWithFinished(confirmedSoFar));

            List<ChangeStreamEvent> batch = gate.drainConfirmedPrefix();
            assertEquals(1, batch.size(), "Expected exactly segment " + i + " to be released");
            assertSame(mis[i], batch.get(0));
        }

        assertTrue(gate.isEmpty());
    }

    @Test
    void isFull_whenTotalEventCountMeetsOrExceedsMax() {
        MoveInBufferGate gate = new MoveInBufferGate(DEST, 3, () -> ctxEmpty());

        PartitionEventEvent mi1 = moveInEvent();
        gate.addMoveIn(T1, List.of("src1"), mi1, fakeMeta()); // size = 1 (MoveIn header)
        assertFalse(gate.isFull());

        gate.addDataEvent(dataEvent()); // size = 2
        assertFalse(gate.isFull());

        gate.addDataEvent(dataEvent()); // size = 3 >= max=3
        assertTrue(gate.isFull());
    }

    @Test
    void getFirstMoveInEvent_updatesAfterPrefixDrain() {
        AtomicReference<TaskSyncContext> ctxRef = new AtomicReference<>(ctxEmpty());
        MoveInBufferGate gate = new MoveInBufferGate(DEST, MAX_EVENTS, ctxRef::get);

        PartitionEventEvent mi1 = moveInEvent();
        PartitionEventEvent mi2 = moveInEvent();
        gate.addMoveIn(T1, List.of("src1"), mi1, fakeMeta());
        gate.addMoveIn(T2, List.of("src2"), mi2, fakeMeta());

        // Initially: first remaining segment is Segment 1
        assertSame(mi1, gate.getFirstMoveInEvent());

        // After confirming src1 and draining Segment 1:
        ctxRef.set(ctxWithFinished("src1"));
        gate.drainConfirmedPrefix();

        // Now first remaining segment is Segment 2
        assertSame(mi2, gate.getFirstMoveInEvent());
    }

    @Test
    void getFirstMoveInEvent_returnsNullWhenEmpty() {
        MoveInBufferGate gate = new MoveInBufferGate(DEST, MAX_EVENTS, () -> ctxEmpty());
        assertNull(gate.getFirstMoveInEvent());
        assertNull(gate.getFirstMoveInMetadata());
    }

    @Test
    void getAllSources_returnsDeduplicatedUnionAcrossAllSegments() {
        MoveInBufferGate gate = new MoveInBufferGate(DEST, MAX_EVENTS, () -> ctxEmpty());

        // src2 appears in both segments
        gate.addMoveIn(T1, List.of("src1", "src2"), moveInEvent(), fakeMeta());
        gate.addMoveIn(T2, List.of("src2", "src3"), moveInEvent(), fakeMeta());

        List<String> all = gate.getAllSources();
        assertEquals(3, all.size());
        assertTrue(all.containsAll(List.of("src1", "src2", "src3")));
    }

    /**
     * Same-timestamp MoveIn events (partition merge): two MoveIn events at identical
     * commit timestamp T must be coalesced into one segment so that data events
     * between them are not released until BOTH sources have confirmed.
     */
    @Test
    void sameTimestampMoveIns_coalesceIntoOneSegment_requireBothSourcesToConfirm() {
        AtomicReference<TaskSyncContext> ctxRef = new AtomicReference<>(ctxEmpty());
        MoveInBufferGate gate = new MoveInBufferGate(DEST, MAX_EVENTS, ctxRef::get);

        PartitionEventEvent mi1 = moveInEvent();
        PartitionEventEvent mi2 = moveInEvent();
        ChangeStreamEvent d1 = dataEvent(); // arrives between MI1 and MI2 (same T)
        ChangeStreamEvent d2 = dataEvent(); // arrives after MI2

        gate.addMoveIn(T1, List.of("src1"), mi1, fakeMeta()); // Segment opened at T1
        gate.addDataEvent(d1);
        gate.addMoveIn(T1, List.of("src2"), mi2, fakeMeta()); // Same T1 → coalesced into existing segment
        gate.addDataEvent(d2);

        // One coalesced segment: sources={src1,src2}, dataEvents=[d1, d2]
        assertEquals(3, gate.size()); // 1 MoveIn header + 2 data events (second MoveIn merged, not a new header)
        assertFalse(gate.isEmpty());

        // Only src1 confirmed: segment must NOT release because src2 is still pending
        ctxRef.set(ctxWithFinished("src1"));
        assertTrue(gate.drainConfirmedPrefix().isEmpty());
        assertFalse(gate.isEmpty());

        // Both confirmed: segment releases with mi1 header + d1 + d2
        ctxRef.set(ctxWithFinished("src1", "src2"));
        List<ChangeStreamEvent> ready = gate.drainConfirmedPrefix();
        assertEquals(List.of(mi1, d1, d2), ready);
        assertTrue(gate.isEmpty());
    }

    @Test
    void size_andIsEmpty_reflectDrainedSegments() {
        AtomicReference<TaskSyncContext> ctxRef = new AtomicReference<>(ctxEmpty());
        MoveInBufferGate gate = new MoveInBufferGate(DEST, MAX_EVENTS, ctxRef::get);

        gate.addMoveIn(T1, List.of("src1"), moveInEvent(), fakeMeta());
        gate.addDataEvent(dataEvent());
        gate.addDataEvent(dataEvent());
        // size = 3 (1 MoveIn + 2 data)
        assertEquals(3, gate.size());
        assertFalse(gate.isEmpty());

        ctxRef.set(ctxWithFinished("src1"));
        gate.drainConfirmedPrefix();

        assertEquals(0, gate.size());
        assertTrue(gate.isEmpty());
    }
}
