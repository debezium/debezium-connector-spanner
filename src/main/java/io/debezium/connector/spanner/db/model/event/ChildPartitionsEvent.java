/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import java.util.List;
import java.util.Objects;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.ChildPartition;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;

/**
 * Specific DTO for Spanner Change Stream Child event
 */
public class ChildPartitionsEvent implements ChangeStreamEvent {

    private final Timestamp startTimestamp;

    private final String recordSequence;
    private final List<ChildPartition> childPartitions;
    private final StreamEventMetadata metadata;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChildPartitionsEvent that = (ChildPartitionsEvent) o;

        if (!Objects.equals(startTimestamp, that.startTimestamp)) {
            return false;
        }
        if (!Objects.equals(recordSequence, that.recordSequence)) {
            return false;
        }
        if (!Objects.equals(childPartitions, that.childPartitions)) {
            return false;
        }
        return Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        int result = startTimestamp != null ? startTimestamp.hashCode() : 0;
        result = 31 * result + (recordSequence != null ? recordSequence.hashCode() : 0);
        result = 31 * result + (childPartitions != null ? childPartitions.hashCode() : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }

    /**
     * Constructs a child partitions record containing one or more child partitions.
     *
     * @param startTimestamp  the timestamp which this partition started being valid in Cloud Spanner
     * @param recordSequence  the order within a partition and a transaction in which the record was
     *                        put to the stream
     * @param childPartitions child partition tokens emitted within this record
     * @param metadata        connector execution metadata for the given record
     */
    public ChildPartitionsEvent(
                                Timestamp startTimestamp,
                                String recordSequence,
                                List<ChildPartition> childPartitions,
                                StreamEventMetadata metadata) {
        this.startTimestamp = startTimestamp;
        this.recordSequence = recordSequence;
        this.childPartitions = childPartitions;
        this.metadata = metadata;
    }

    /**
     * Returns the start timestamp of the child partition. The
     * caller must use this time as the change stream query start timestamp for the new partitions.
     *
     * @return the start timestamp of the partition
     */
    @Override
    public Timestamp getRecordTimestamp() {
        return getStartTimestamp();
    }

    /**
     * It is the partition_start_time of the child partition token. This partition_start_time is
     * guaranteed to be the same across all the child partitions yielded from a parent. When users
     * start new queries with the child partition tokens, the returned records must have a timestamp
     * >= partition_start_time.
     *
     * @return the start timestamp of the partition
     */
    public Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Indicates the order in which a record was put to the stream. Is unique and increasing within a
     * partition. It is relative to the scope of partition, commit timestamp, and
     * server_transaction_id. It is useful for readers downstream to deduplicate any records that
     * were read/recorded.
     *
     * @return record sequence of the record
     */
    public String getRecordSequence() {
        return recordSequence;
    }

    /**
     * List of child partitions yielded within this record.
     *
     * @return child partitions
     */
    public List<ChildPartition> getChildPartitions() {
        return childPartitions;
    }

    @Override
    public StreamEventMetadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "ChildPartitionsEvent{"
                + "startTimestamp="
                + startTimestamp
                + ", recordSequence='"
                + recordSequence
                + '\''
                + ", childPartitions="
                + childPartitions
                + ", metadata="
                + metadata
                + '}';
    }
}
