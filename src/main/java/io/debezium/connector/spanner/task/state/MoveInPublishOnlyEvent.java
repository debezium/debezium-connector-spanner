/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.state;

import java.util.List;

import com.google.cloud.Timestamp;

/**
 * Notifies that a mutable key range destination partition has entered the buffer-gate
 * path after detecting a MoveIn event.  Unlike {@link MoveInNotificationEvent} this
 * event does <em>not</em> transition the partition to {@code CREATED}; instead the
 * partition remains in its current streaming state while the streaming thread self-gates
 * using a {@link io.debezium.connector.spanner.db.stream.MoveInBufferGate}.
 *
 * <p>The event still causes the current {@code MoveInState} to be published to the sync
 * topic so that cross-task gate checks and
 * {@link io.debezium.connector.spanner.task.operation.RemoveFinishedPartitionOperation}
 * have the information they need to retain source partition state until the destination
 * has caught up.
 *
 * <p>The {@code isFirstMoveIn} flag distinguishes the first MoveIn event in a buffer
 * sequence from subsequent ones at later timestamps.  Only the first event's timestamp
 * and record sequence are persisted to {@code processedTimestamp} /
 * {@code lastBoundaryRecordSequence} so that crash-recovery can restart the query from
 * the correct boundary without re-emitting events that were already forwarded before the
 * gate activated.
 */
public class MoveInPublishOnlyEvent implements TaskStateChangeEvent {

    private final String token;
    private final Timestamp commitTimestamp;
    private final String recordSequence;
    private final List<String> sourcePartitionTokens;
    private final boolean isFirstMoveIn;

    public MoveInPublishOnlyEvent(String token, Timestamp commitTimestamp, String recordSequence,
                                  List<String> sourcePartitionTokens, boolean isFirstMoveIn) {
        this.token = token;
        this.commitTimestamp = commitTimestamp;
        this.recordSequence = recordSequence;
        this.sourcePartitionTokens = sourcePartitionTokens;
        this.isFirstMoveIn = isFirstMoveIn;
    }

    public String getToken() {
        return token;
    }

    public Timestamp getCommitTimestamp() {
        return commitTimestamp;
    }

    public String getRecordSequence() {
        return recordSequence;
    }

    public List<String> getSourcePartitionTokens() {
        return sourcePartitionTokens;
    }

    /**
     * {@code true} for the first MoveIn event in a gate sequence; {@code false} for
     * subsequent MoveIn events at later timestamps.
     */
    public boolean isFirstMoveIn() {
        return isFirstMoveIn;
    }
}
