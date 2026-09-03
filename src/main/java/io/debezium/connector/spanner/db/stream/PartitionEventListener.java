/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.util.List;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;

/**
 * A listener for the various state querying partition. Used in {@link ChangeStream}
 */
public interface PartitionEventListener {
    void onRun(Partition partition) throws InterruptedException;

    void onFinish(Partition partition);

    void onException(Partition partition, Exception ex) throws InterruptedException;

    boolean onStuckPartition(String token) throws InterruptedException;

    void onWindowAdvanced(Partition partition, Timestamp windowEnd, String lastBoundaryRecordSequence) throws InterruptedException;

    void onMoveIn(Partition partition, Timestamp commitTimestamp, String recordSequence, List<String> sourcePartitionTokens) throws InterruptedException;

    /**
     * Called by the streaming thread when a MoveIn event is handled via the buffer-gate
     * path rather than the standard close/reopen path.  The partition does NOT transition
     * to {@code CREATED}; the streaming thread stays alive and self-gates.  The call
     * publishes {@code MoveInState} to the sync topic for cross-task visibility and
     * crash-recovery purposes.
     *
     * <p>A default no-op is provided so that existing anonymous-class implementations in
     * tests and other callsites do not need to be updated.
     *
     * @param isFirstMoveIn {@code true} for the first MoveIn in a buffer sequence;
     *                      {@code false} for subsequent MoveIn events at later timestamps.
     *                      Only the first event's timestamp / record sequence are used to
     *                      pin {@code processedTimestamp} for boundary deduplication on
     *                      crash-recovery.
     */
    default void onMoveInPublishOnly(Partition partition, Timestamp commitTimestamp, String recordSequence,
                                     List<String> sourcePartitionTokens, boolean isFirstMoveIn)
            throws InterruptedException {
        // no-op by default; overridden by SpannerStreamingChangeEventSource
    }
}
