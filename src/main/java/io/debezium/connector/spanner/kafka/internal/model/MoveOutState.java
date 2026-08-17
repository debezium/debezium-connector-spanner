/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

import java.util.List;

import com.google.cloud.Timestamp;

/**
 * Tracks the MoveOut event state for a source partition in a mutable key range change stream.
 * Stored in {@link PartitionState} and propagated via the sync topic so that destination
 * partitions can verify that the source has processed past the MoveIn commit timestamp
 * before resuming their own queries.
 */
public class MoveOutState {

    private final Timestamp timestamp;
    private final List<String> destPartitionTokens;

    public MoveOutState(Timestamp timestamp, List<String> destPartitionTokens) {
        this.timestamp = timestamp;
        this.destPartitionTokens = destPartitionTokens;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public List<String> getDestPartitionTokens() {
        return destPartitionTokens;
    }

    @Override
    public String toString() {
        return "MoveOutState{" +
                "timestamp=" + timestamp +
                ", destPartitionTokens=" + destPartitionTokens +
                '}';
    }
}
