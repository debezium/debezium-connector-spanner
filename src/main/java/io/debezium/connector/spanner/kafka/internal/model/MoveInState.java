/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

import java.util.List;

import com.google.cloud.Timestamp;

/**
 * Tracks the MoveIn event state for a destination partition in a mutable key range change stream.
 * Stored in {@link PartitionState} and propagated via the sync topic so that the
 * destination partition can resume from the correct timestamp and record sequence
 * after all source partitions have processed past the MoveIn commit timestamp.
 */
public class MoveInState {

    private final Timestamp timestamp;
    private final String recordSequence;
    private final List<String> sourcePartitionTokens;

    public MoveInState(Timestamp timestamp, String recordSequence, List<String> sourcePartitionTokens) {
        this.timestamp = timestamp;
        this.recordSequence = recordSequence;
        this.sourcePartitionTokens = sourcePartitionTokens;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public String getRecordSequence() {
        return recordSequence;
    }

    public List<String> getSourcePartitionTokens() {
        return sourcePartitionTokens;
    }

    @Override
    public String toString() {
        return "MoveInState{" +
                "timestamp=" + timestamp +
                ", recordSequence='" + recordSequence + '\'' +
                ", sourcePartitionTokens=" + sourcePartitionTokens +
                '}';
    }
}
