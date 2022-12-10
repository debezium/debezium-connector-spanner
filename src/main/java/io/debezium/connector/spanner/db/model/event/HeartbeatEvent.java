/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;

/**
 * Specific DTO for Spanner Heartbeat event
 */
public class HeartbeatEvent implements ChangeStreamEvent {

    private final Timestamp timestamp;

    private final StreamEventMetadata metadata;

    /**
     * Constructs the heartbeat record with the given timestamp and metadata.
     *
     * @param timestamp the timestamp for which all changes in the partition have occurred
     * @param metadata  connector execution metadata for the given record
     */
    public HeartbeatEvent(Timestamp timestamp, StreamEventMetadata metadata) {
        this.timestamp = timestamp;
        this.metadata = metadata;
    }

    /**
     * Indicates the timestamp for which the change stream query has returned all changes. All records
     * emitted after the heartbeat record will have a timestamp greater than this one.
     *
     * @return the timestamp for which the change stream query has returned all changes
     */
    @Override
    public Timestamp getRecordTimestamp() {
        return getTimestamp();
    }

    /**
     * Indicates the timestamp for which the change stream query has returned all changes. All records
     * emitted after the heartbeat record will have a timestamp greater than this one.
     *
     * @return the timestamp for which the change stream query has returned all changes
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public StreamEventMetadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "HeartbeatEvent{" + "timestamp=" + timestamp + ", metadata=" + metadata + '}';
    }
}
