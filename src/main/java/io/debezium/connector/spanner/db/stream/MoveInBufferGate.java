/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.dao.ChangeStreamResultSetMetadata;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEventEvent;
import io.debezium.connector.spanner.task.MoveInGateChecker;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Per-partition buffer that accumulates {@link ChangeStreamEvent}s received after a MoveIn
 * boundary while the destination partition waits for source partition(s) to confirm
 * their corresponding MoveOut event(s) via the sync topic.
 *
 * <p>Events are organised into <em>segments</em>, one per MoveIn event encountered.
 * Each segment stores the MoveIn event itself as its header, plus all non-MoveIn
 * events (data, heartbeats) that arrived from the Spanner stream after that MoveIn
 * but before the next one.  Segments are held in arrival order and released from the
 * head: {@link #drainConfirmedPrefix()} walks from the oldest segment and flushes each
 * one whose specific source partitions have confirmed their MoveOut, stopping at the
 * first unconfirmed segment.
 *
 * <p>This eliminates the convoy / head-of-line-blocking problem that a single
 * all-or-nothing release across all accumulated MoveIn entries would cause: a slow or
 * continuously re-splitting source can only delay its own segment, not earlier segments
 * whose sources have already confirmed.
 *
 * <p>The underlying Spanner gRPC connection <em>stays open</em> throughout; events are
 * held here rather than being forwarded to the downstream blocking
 * {@link io.debezium.connector.spanner.StreamEventQueue}.  When {@link #isEmpty()}
 * returns {@code true} all segments have been drained and the gate can be discarded.
 *
 * <p>When {@link #isFull()} returns {@code true} the caller must fall back to the
 * existing close/reopen path.  The {@link #getFirstMoveInEvent()} and
 * {@link #getFirstMoveInMetadata()} accessors supply the values needed for that path.
 */
public class MoveInBufferGate {

    /**
     * One segment per MoveIn event seen while this gate is active.
     *
     * <p>The MoveIn event is stored as the segment header and is forwarded to the
     * downstream consumer when the segment is released (it is a no-op there, but
     * must be forwarded to maintain the exact same event sequence as the original
     * non-gate path).  Non-MoveIn events (data changes, heartbeats) that arrive after
     * this MoveIn but before the next one are accumulated in {@code dataEvents}.
     *
     * <p>A segment is released when every source token in {@code sources} has
     * confirmed its MoveOut via {@link MoveInGateChecker#canContinue} — independently
     * of whether any later segment has confirmed.
     */
    private static final class Segment {
        final Timestamp moveInTs;
        final Set<String> sources;
        final PartitionEventEvent moveInEvent;
        final ChangeStreamResultSetMetadata moveInMetadata;
        /** Non-MoveIn events that arrived after this MoveIn, in stream order. */
        final List<ChangeStreamEvent> dataEvents = new ArrayList<>();

        Segment(Timestamp moveInTs, Set<String> sources,
                PartitionEventEvent moveInEvent, ChangeStreamResultSetMetadata moveInMetadata) {
            this.moveInTs = moveInTs;
            this.sources = sources;
            this.moveInEvent = moveInEvent;
            this.moveInMetadata = moveInMetadata;
        }
    }

    /**
     * Ordered queue of segments: new segments appended at the tail by
     * {@link #addMoveIn}; confirmed segments removed from the head by
     * {@link #drainConfirmedPrefix}.  Using {@code ArrayDeque} gives O(1)
     * head-removal, which is the hot path when events arrive faster than sources
     * confirm.
     */
    private final ArrayDeque<Segment> segments = new ArrayDeque<>();

    private final String destToken;
    private final int maxBufferEvents;
    private final Supplier<TaskSyncContext> taskSyncContextSupplier;

    public MoveInBufferGate(String destToken, int maxBufferEvents,
                            Supplier<TaskSyncContext> taskSyncContextSupplier) {
        this.destToken = destToken;
        this.maxBufferEvents = maxBufferEvents;
        this.taskSyncContextSupplier = taskSyncContextSupplier;
    }

    /**
     * Opens a new segment for the given MoveIn event.  All subsequent
     * {@link #addDataEvent} calls accumulate into this segment until the next
     * {@code addMoveIn} call opens the next one.
     *
     * @param ts           commit timestamp of the MoveIn event
     * @param sourceTokens source partition tokens listed in the MoveIn record
     * @param event        the raw {@link PartitionEventEvent}
     * @param metadata     result-set metadata at the time the event was read
     */
    public void addMoveIn(Timestamp ts, List<String> sourceTokens,
                          PartitionEventEvent event, ChangeStreamResultSetMetadata metadata) {
        Segment last = segments.peekLast();
        if (last != null && last.moveInTs.equals(ts)) {
            // Same commit timestamp: two source partitions merging into one destination.
            // Coalesce into the existing segment so that all same-timestamp sources must
            // confirm before the segment is released — preserving the ordering guarantee
            // for the merged key range boundary.
            last.sources.addAll(sourceTokens);
        }
        else {
            segments.addLast(new Segment(ts, new LinkedHashSet<>(sourceTokens), event, metadata));
        }
    }

    /**
     * Appends a non-MoveIn event (data change or heartbeat) to the current (latest)
     * segment.  Must only be called after at least one {@link #addMoveIn} call.
     */
    public void addDataEvent(ChangeStreamEvent event) {
        segments.peekLast().dataEvents.add(event);
    }

    /**
     * Returns {@code true} when all segments have been drained — there is nothing left
     * to release and the gate can be discarded.
     */
    public boolean isEmpty() {
        return segments.isEmpty();
    }

    /**
     * Returns the total number of events held across all segments:
     * one MoveIn event header per segment plus all accumulated data events.
     */
    public int size() {
        int count = segments.size(); // one MoveIn event per segment header
        for (Segment seg : segments) {
            count += seg.dataEvents.size();
        }
        return count;
    }

    /**
     * Returns {@code true} when the total buffered event count has reached capacity.
     * The caller must then fall back to the existing close/reopen path.
     */
    public boolean isFull() {
        return size() >= maxBufferEvents;
    }

    /**
     * Walks segments from oldest to newest.  For each segment whose source partitions
     * have all confirmed their MoveOut (checked via {@link MoveInGateChecker#canContinue}),
     * collects its MoveIn event followed by its data events in arrival order, removes
     * the segment, and continues to the next.  Stops at the first unconfirmed segment.
     *
     * <p>Returns all events from the confirmed prefix in arrival order.
     * Returns an empty list when no prefix can yet be released.
     *
     * <p>The {@link TaskSyncContext} snapshot is read once per call via the injected
     * {@link java.util.concurrent.atomic.AtomicReference}-backed supplier; the call is
     * wait-free.
     */
    public List<ChangeStreamEvent> drainConfirmedPrefix() {
        if (segments.isEmpty()) {
            return List.of();
        }
        TaskSyncContext ctx = taskSyncContextSupplier.get();
        Set<String> finished = MoveInGateChecker.getFinishedPartitions(ctx);

        List<ChangeStreamEvent> result = new ArrayList<>();
        while (!segments.isEmpty()) {
            Segment seg = segments.peekFirst();
            if (!MoveInGateChecker.canContinue(ctx, destToken, seg.moveInTs,
                    new ArrayList<>(seg.sources), finished)) {
                break; // oldest segment not yet confirmed — stop here
            }
            segments.pollFirst(); // remove confirmed head
            result.add(seg.moveInEvent); // MoveIn event first (preserves stream order)
            result.addAll(seg.dataEvents);
        }
        return result;
    }

    /**
     * Returns an ordered snapshot of (timestamp → sources) for all remaining segments,
     * intended for logging.
     */
    public Map<Timestamp, Set<String>> getSourcesByTimestamp() {
        Map<Timestamp, Set<String>> result = new LinkedHashMap<>();
        for (Segment seg : segments) {
            result.computeIfAbsent(seg.moveInTs, k -> new LinkedHashSet<>()).addAll(seg.sources);
        }
        return result;
    }

    /** First MoveIn event in the oldest remaining segment; used by the overflow-fallback path. */
    public PartitionEventEvent getFirstMoveInEvent() {
        Segment first = segments.peekFirst();
        return first == null ? null : first.moveInEvent;
    }

    /** Metadata of the oldest remaining MoveIn event; used for latency-metric logging on fallback. */
    public ChangeStreamResultSetMetadata getFirstMoveInMetadata() {
        Segment first = segments.peekFirst();
        return first == null ? null : first.moveInMetadata;
    }

    /**
     * Returns the de-duplicated union of all source partition tokens across every
     * remaining segment.  Used by the overflow-fallback and interrupt-fallback paths
     * so that {@code MoveInStateUpdateOperation} waits for every source, not only
     * those of the first MoveIn event.
     */
    public List<String> getAllSources() {
        return segments.stream()
                .flatMap(seg -> seg.sources.stream())
                .distinct()
                .collect(Collectors.toList());
    }
}
